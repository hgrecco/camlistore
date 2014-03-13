// +build linux darwin

/*
Copyright 2012 Google Inc.

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

package fs

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/search"
	"camlistore.org/pkg/syncutil"
	"camlistore.org/third_party/bazil.org/fuse"
	"camlistore.org/third_party/bazil.org/fuse/fs"
)

const versionsRefreshTime = 1 * time.Minute

type versionsDir struct {
	noXattr
	fs *CamliFileSystem

	mu          sync.Mutex
	lastQuery time.Time
	ents        map[string]*search.DescribedBlob // filename to blob meta
	modTime     map[string]time.Time             // filename to permanode modtime
	m           map[string]blob.Ref // ent name => permanode
	children    map[string]fs.Node  // ent name => child node
}

func (n *versionsDir) isRO() bool {
	return true;
}

func (n *versionsDir) dirMode() os.FileMode {
	if n.isRO() {
		return 0500
	}
	return 0700
}

func (n *versionsDir) Attr() fuse.Attr {
	return fuse.Attr{
		Mode: os.ModeDir | n.dirMode(),
		Uid:  uint32(os.Getuid()),
		Gid:  uint32(os.Getgid()),
	}
}

func (n *versionsDir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.condRefresh(); err != nil {
		return nil, fuse.EIO
	}
	var ents []fuse.Dirent
	for name := range n.m {
		ents = append(ents, fuse.Dirent{Name: name})
	}
	log.Printf("fs.versions.ReadDir() -> %v", ents)
	return ents, nil
}

func (n *versionsDir) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	log.Printf("fs.versions: Lookup(%q)", name)
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.condRefresh(); err != nil {
		return nil, err
	}
	br := n.m[name]
	if !br.Valid() {
		return nil, fuse.ENOENT
	}

	nod, ok := n.children[name]
	
	if ok {
		return nod, nil
	}

	nod = newRODirV(n.fs, br, name)

	n.children[name] = nod

	return nod, nil
}

// requires n.mu is held
func (n *versionsDir) condRefresh() fuse.Error {
	if n.lastQuery.After(time.Now().Add(-versionsRefreshTime)) {
		return nil
	}
	log.Printf("fs.versions: querying")

	var rootRes, impRes *search.WithAttrResponse
	var grp syncutil.Group
	grp.Go(func() (err error) {
		rootRes, err = n.fs.client.GetPermanodesWithAttr(&search.WithAttrRequest{N: 100, Attr: "camliRoot"})
		return
	})
	grp.Go(func() (err error) {
		impRes, err = n.fs.client.GetPermanodesWithAttr(&search.WithAttrRequest{N: 100, Attr: "camliImportRoot"})
		return
	})
	if err := grp.Err(); err != nil {
		log.Printf("fs.versions: GetRecentPermanodes error in ReadDir: %v", err)
		return fuse.EIO
	}

	n.m = make(map[string]blob.Ref)
	if n.children == nil {
		n.children = make(map[string]fs.Node)
	}

	dr := &search.DescribeRequest{
		Depth: 1,
	}
	for _, wi := range rootRes.WithAttr {
		dr.BlobRefs = append(dr.BlobRefs, wi.Permanode)
	}
	for _, wi := range impRes.WithAttr {
		dr.BlobRefs = append(dr.BlobRefs, wi.Permanode)
	}
	if len(dr.BlobRefs) == 0 {
		return nil
	}

	dres, err := n.fs.client.Describe(dr)
	if err != nil {
		log.Printf("Describe failure: %v", err)
		return fuse.EIO
	}

	// Roots
	currentRoots := map[string]bool{}
	for _, wi := range rootRes.WithAttr {
		pn := wi.Permanode
		db := dres.Meta[pn.String()]
		if db != nil && db.Permanode != nil {
			name := db.Permanode.Attr.Get("camliRoot")
			if name != "" {
				currentRoots[name] = true
				n.m[name] = pn
			}
		}
	}

	// Remove any children objects we have mapped that are no
	// longer relevant.
	for name := range n.children {
		if !currentRoots[name] {
			delete(n.children, name)
		}
	}

	// Importers (mapped as roots for now)
	for _, wi := range impRes.WithAttr {
		pn := wi.Permanode
		db := dres.Meta[pn.String()]
		if db != nil && db.Permanode != nil {
			name := db.Permanode.Attr.Get("camliImportRoot")
			if name != "" {
				name = strings.Replace(name, ":", "-", -1)
				name = strings.Replace(name, "/", "-", -1)
				n.m["importer-"+name] = pn
			}
		}
	}

	n.lastQuery = time.Now()
	return nil
}

