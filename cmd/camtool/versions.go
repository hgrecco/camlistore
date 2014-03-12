/*
Copyright 2014 The Camlistore Authors.

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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/cmdmain"
	"camlistore.org/pkg/search"
)

type versionsCmd struct {
	server   string
	limit    int
	describe bool
}

func init() {
	cmdmain.RegisterCommand("versions", func(flags *flag.FlagSet) cmdmain.CommandRunner {
		cmd := new(versionsCmd)
		flags.StringVar(&cmd.server, "server", "", "Server to search. "+serverFlagHelp)
		//TODO (hgrecco): Implement limit
		//flags.IntVar(&cmd.limit, "limit", 0, "Limit number of results. 0 is default. Negative means no limit.")
		return cmd
	})
}

func (c *versionsCmd) Describe() string {
	return "Execute a versions query"
}

func (c *versionsCmd) Usage() {
	fmt.Fprintf(os.Stderr, "Usage: camtool [globalopts] versions <permanode>\n")
}

func (c *versionsCmd) Examples() []string {
	return []string{
		`sha1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx # a permanode` ,
		`- # piped from stdin`,
	}
}

func (c *versionsCmd) RunCommand(args []string) error {
	if len(args) != 1 {
		return cmdmain.UsageError("requires permanode")
	}
	pn := args[0]
	if pn == "-" {
		slurp, err := ioutil.ReadAll(cmdmain.Stdin)
		if err != nil {
			return err
		}
		pn = string(slurp)
	}
	pn = strings.TrimSpace(pn)

	br, ok := blob.Parse(pn)
	if !ok {
		return cmdmain.UsageError(fmt.Sprintf("invalid blobref %q", pn))
	}

	cl := newClient(c.server)
	res, err := cl.GetClaims(&search.ClaimsRequest{br, "camliContent"})
	if err != nil {
		return err
	}
	
	resj, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	resj = append(resj, '\n')
	_, err = os.Stdout.Write(resj)
	return err
}
