//go:build !fdb

package main

import (
	"errors"

	"github.com/ipni/dhstore"
)

func newFDBDHStore() (dhstore.DHStore, error) {
	return nil, errors.New("dhstore built without fdb support")
}
