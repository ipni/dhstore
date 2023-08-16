//go:build !nofdb

package main

import (
	"flag"

	"github.com/ipni/dhstore"
	"github.com/ipni/dhstore/fdb"
)

var fdbApiVersion *int
var fdbClusterFile *string

func init() {
	fdbApiVersion = flag.Int("fdbApiVersion", 0, "Required. The FoundationDB API version as a numeric value")
	fdbClusterFile = flag.String("fdbClusterFile", "", "Required. Path to ")
}

func newFDBDHStore() (dhstore.DHStore, error) {
	return fdb.NewFDBDHStore(fdb.WithApiVersion(*fdbApiVersion), fdb.WithClusterFile(*fdbClusterFile))
}
