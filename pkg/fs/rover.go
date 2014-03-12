// +build linux darwin

/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file implements versionated versions of read-only dir and file.

package fs

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/search"
	"camlistore.org/pkg/schema"

	"camlistore.org/third_party/bazil.org/fuse"
	"camlistore.org/third_party/bazil.org/fuse/fs"
)

// roDirV is a read-only directory with versionated files.
// It is EXACTLY the same as roDir, except that it uses 
//  roFileLikeDir except of roFile. There might be a way to reuse
//  roDir
// Its permanode is the permanode with camliPath:entname attributes.
type roDirV struct {
	fs        *CamliFileSystem
	permanode blob.Ref
	parent    *roDirV // or nil, if the root within its roots.go root.
	name      string // ent name (base name within parent)

	mu       sync.Mutex
	children map[string]roFileOrDir
	xattrs   map[string][]byte
}

func newRODirV(fs *CamliFileSystem, permanode blob.Ref, name string) *roDirV {
	return &roDirV{
		fs:        fs,
		permanode: permanode,
		name:      name,
	}
}

// for debugging
func (n *roDirV) fullPath() string {
	if n == nil {
		return ""
	}
	return filepath.Join(n.parent.fullPath(), n.name)
}

func (n *roDirV) Attr() fuse.Attr {
	return fuse.Attr{
		Inode: n.permanode.Sum64(),
		Mode:  os.ModeDir | 0500,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
	}
}

// populate hits the blobstore to populate map of child nodes.
func (n *roDirV) populate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Things never change here, so if we've ever populated, we're
	// populated.
	if n.children != nil {
		return nil
	}

	log.Printf("roDirV.populate(%q)", n.fullPath())

	res, err := n.fs.client.Describe(&search.DescribeRequest{
		BlobRef: n.permanode,
		Depth:   3,
	})
	if err != nil {
		log.Println("roDirV.paths:", err)
		return nil
	}
	db := res.Meta[n.permanode.String()]
	if db == nil {
		return errors.New("dir blobref not described")
	}

	// Find all child permanodes and stick them in n.children
	n.children = make(map[string]roFileOrDir)
	for k, v := range db.Permanode.Attr {
		const p = "camliPath:"
		if !strings.HasPrefix(k, p) || len(v) < 1 {
			continue
		}
		name := k[len(p):]
		childRef := v[0]
		child := res.Meta[childRef]
		if child == nil {
			log.Printf("child not described: %v", childRef)
			continue
		}
		if target := child.Permanode.Attr.Get("camliSymlinkTarget"); target != "" {
			// This is a symlink.
			n.children[name] = &roFileLikeDir{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
				symLink:   true,
				target:    target,
			}
		} else if isDir(child.Permanode) {
			// This is a directory.
			n.children[name] = &roDirV{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
			}
		} else if contentRef := child.Permanode.Attr.Get("camliContent"); contentRef != "" {
			// This is a file.
			content := res.Meta[contentRef]
			if content == nil {
				log.Printf("child content not described: %v", childRef)
				continue
			}
			if content.CamliType != "file" {
				log.Printf("child not a file: %v", childRef)
				continue
			}
			n.children[name] = &roFileLikeDir{
				fs:        n.fs,
				permanode: blob.ParseOrZero(childRef),
				parent:    n,
				name:      name,
			}
		} else {
			// unknown type
			continue
		}
		n.children[name].xattr().load(child.Permanode)
	}
	return nil
}

func (n *roDirV) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, fuse.EIO
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var ents []fuse.Dirent
	for name, childNode := range n.children {
		var ino uint64
		switch v := childNode.(type) {
		case *roDirV:
			ino = v.permanode.Sum64()
		case *roFileVersion:
			ino = v.permanode.Sum64()
		default:
			log.Printf("roDirV.ReadDir: unknown child type %T", childNode)
		}

		// TODO: figure out what Dirent.Type means.
		// fuse.go says "Type uint32 // ?"
		dirent := fuse.Dirent{
			Name:  name,
			Inode: ino,
		}
		log.Printf("roDirV(%q) appending inode %x, %+v", n.fullPath(), dirent.Inode, dirent)
		ents = append(ents, dirent)
	}
	return ents, nil
}

func (n *roDirV) Lookup(name string, intr fs.Intr) (ret fs.Node, err fuse.Error) {
	defer func() {
		log.Printf("roDirV(%q).Lookup(%q) = %#v, %v", n.fullPath(), name, ret, err)
	}()
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, fuse.EIO
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n2 := n.children[name]; n2 != nil {
		return n2, nil
	}
	return nil, fuse.ENOENT
}


// roFileLikeDir is a read-only file that appears as a directory
// The content of the directory are the dates in which the file was
//  modified (with a claim that changed camliContent)
type roFileLikeDir struct {
	fs        *CamliFileSystem
	permanode blob.Ref
	parent    *roDirV // or nil, if the root within its roots.go root.
	name      string // ent name (base name within parent)

	symLink      bool       // if true, is a symlink
	target       string     // if a symlink
	
	mu       sync.Mutex
	children map[string]roFileOrDir
	xattrs   map[string][]byte
}

func newROFileLikeDir(fs *CamliFileSystem, permanode blob.Ref, name string) *roFileLikeDir {
	return &roFileLikeDir{
		fs:        fs,
		permanode: permanode,
		name:      name,
	}
}

// for debugging
func (n *roFileLikeDir) fullPath() string {
	if n == nil {
		return ""
	}
	return filepath.Join(n.parent.fullPath(), n.name)
}

func (n *roFileLikeDir) Attr() fuse.Attr {
	return fuse.Attr{
		Inode: n.permanode.Sum64(),
		Mode:  os.ModeDir | 0500,
		Uid:   uint32(os.Getuid()),
		Gid:   uint32(os.Getgid()),
	}
}

// populate hits the blobstore to populate map of child nodes.
func (n *roFileLikeDir) populate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Things never change here, so if we've ever populated, we're
	// populated.
	if n.children != nil {
		return nil
	}

	log.Printf("roFileLikeDir.populate(%q)", n.fullPath())

	res, err := n.fs.client.GetClaims(&search.ClaimsRequest{n.permanode, "camliContent"})
	if err != nil {
		log.Printf("fs.roFileLikeDir: GetClaims error in ReadDir: %v", err)
		return errors.New("fs.roFileLikeDir: GetClaims error in ReadDir:")
	}

	n.children = make(map[string]roFileOrDir)
	for _, cl := range res.Claims {
		pn, ok := blob.Parse(cl.Value)
		if !ok {
			return errors.New("invalid blobref")
		}
		res, err := n.fs.client.Describe(&search.DescribeRequest{
			BlobRef: pn, // this is camliContent
			Depth:   1,
			At:      cl.Date,
		})
		if err != nil {
			log.Println("roDir.paths:", err)
			return nil
		}
		db := res.Meta[cl.Value]
		if db == nil {
			return errors.New("dir blobref not described")
		}
		name := cl.Date.String()
		n.children[name] = &roFileVersion{
			fs:        n.fs,
			permanode: n.permanode,
			parent:    n,
			name:      name,
			content:   db.BlobRef,
			size:      db.File.Size,
		}
			
	}
	return nil
}

func (n *roFileLikeDir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, fuse.EIO
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var ents []fuse.Dirent
	for name, childNode := range n.children {
		var ino uint64
		switch v := childNode.(type) {
		case *roDirV:
			ino = v.permanode.Sum64()
		case *roFileVersion:
			ino = v.permanode.Sum64()
		default:
			log.Printf("roDirV.ReadDir: unknown child type %T", childNode)
		}

		// TODO: figure out what Dirent.Type means.
		// fuse.go says "Type uint32 // ?"
		dirent := fuse.Dirent{
			Name:  name,
			Inode: ino,
		}
		log.Printf("roDirV(%q) appending inode %x, %+v", n.fullPath(), dirent.Inode, dirent)
		ents = append(ents, dirent)
	}
	return ents, nil
}

func (n *roFileLikeDir) Lookup(name string, intr fs.Intr) (ret fs.Node, err fuse.Error) {
	defer func() {
		log.Printf("roDirV(%q).Lookup(%q) = %#v, %v", n.fullPath(), name, ret, err)
	}()
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, fuse.EIO
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n2 := n.children[name]; n2 != nil {
		return n2, nil
	}
	return nil, fuse.ENOENT
}

// roFileVersion is the version of a file
type roFileVersion struct {
	fs        *CamliFileSystem
	permanode blob.Ref
	parent    *roFileLikeDir
	name      string // ent name (base name within parent)

	mu           sync.Mutex // protects all following fields
	symLink      bool       // if true, is a symlink
	target       string     // if a symlink
	content      blob.Ref   // if a regular file
	size         int64
	mtime, atime time.Time // if zero, use serverStart
	xattrs       map[string][]byte
}

// Empirically:
//  open for read:   req.Flags == 0
//  open for append: req.Flags == 1
//  open for write:  req.Flags == 1
//  open for read/write (+<)   == 2 (bitmask? of?)
//
// open flags are O_WRONLY (1), O_RDONLY (0), or O_RDWR (2). and also
// bitmaks of O_SYMLINK (0x200000) maybe. (from
// fuse_filehandle_xlate_to_oflags in macosx/kext/fuse_file.h)
func (n *roFileVersion) Open(req *fuse.OpenRequest, res *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	roFileOpen.Incr()

	if isWriteFlags(req.Flags) {
		return nil, fuse.EPERM
	}

	log.Printf("roFile.Open: %v: content: %v dir=%v flags=%v", n.permanode, n.content, req.Dir, req.Flags)
	r, err := schema.NewFileReader(n.fs.fetcher, n.content)
	if err != nil {
		roFileOpenError.Incr()
		log.Printf("roFile.Open: %v", err)
		return nil, fuse.EIO
	}

	// Turn off the OpenDirectIO bit (on by default in rsc fuse server.go),
	// else append operations don't work for some reason.
	res.Flags &= ^fuse.OpenDirectIO

	// Read-only.
	nod := &node{
		fs:      n.fs,
		blobref: n.content,
	}
	return &nodeReader{n: nod, fr: r}, nil
}

func (n *roDirV) Getxattr(req *fuse.GetxattrRequest, res *fuse.GetxattrResponse, intr fs.Intr) fuse.Error {
	return n.xattr().get(req, res)
}

func (n *roDirV) Listxattr(req *fuse.ListxattrRequest, res *fuse.ListxattrResponse, intr fs.Intr) fuse.Error {
	return n.xattr().list(req, res)
}

func (n *roFileVersion) Getxattr(req *fuse.GetxattrRequest, res *fuse.GetxattrResponse, intr fs.Intr) fuse.Error {
	return n.xattr().get(req, res)
}

func (n *roFileVersion) Listxattr(req *fuse.ListxattrRequest, res *fuse.ListxattrResponse, intr fs.Intr) fuse.Error {
	return n.xattr().list(req, res)
}

func (n *roFileVersion) Removexattr(req *fuse.RemovexattrRequest, intr fs.Intr) fuse.Error {
	return fuse.EPERM
}

func (n *roFileVersion) Setxattr(req *fuse.SetxattrRequest, intr fs.Intr) fuse.Error {
	return fuse.EPERM
}

// for debugging
func (n *roFileVersion) fullPath() string {
	if n == nil {
		return ""
	}
	return filepath.Join(n.parent.fullPath(), n.name)
}

func (n *roFileVersion) Attr() fuse.Attr {
	// TODO: don't grab n.mu three+ times in here.
	var mode os.FileMode = 0400 // read-only
	
	n.mu.Lock()
	size := n.size
	var blocks uint64
	if size > 0 {
		blocks = uint64(size)/512 + 1
	}
	inode := n.permanode.Sum64()
	if n.symLink {
		mode |= os.ModeSymlink
	}
	n.mu.Unlock()

	return fuse.Attr{
		Inode:  inode,
		Mode:   mode,
		Uid:    uint32(os.Getuid()),
		Gid:    uint32(os.Getgid()),
		Size:   uint64(size),
		Blocks: blocks,
		Mtime:  n.modTime(),
		Atime:  n.accessTime(),
		Ctime:  serverStart,
		Crtime: serverStart,
	}
}

func (n *roFileVersion) accessTime() time.Time {
	n.mu.Lock()
	if !n.atime.IsZero() {
		defer n.mu.Unlock()
		return n.atime
	}
	n.mu.Unlock()
	return n.modTime()
}

func (n *roFileVersion) modTime() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.mtime.IsZero() {
		return n.mtime
	}
	return serverStart
}

func (n *roFileVersion) Fsync(r *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	// noop
	return nil
}

func (n *roFileVersion) permanodeString() string {
	return n.permanode.String()
}

func (n *roFileLikeDir) permanodeString() string {
	return n.permanode.String()
}

func (n *roDirV) permanodeString() string {
	return n.permanode.String()
}

func (n *roFileVersion) xattr() *xattr {
	return &xattr{"roFileVersion", n.fs, n.permanode, &n.mu, &n.xattrs}
}

func (n *roFileLikeDir) xattr() *xattr {
	return &xattr{"roFileLikeDir", n.fs, n.permanode, &n.mu, &n.xattrs}
}

func (n *roDirV) xattr() *xattr {
	return &xattr{"roDirV", n.fs, n.permanode, &n.mu, &n.xattrs}
}

