package rwriter

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/apierror"
	"github.com/mr-tron/base58"
	"github.com/multiformats/go-multihash"
)

var newline = []byte("\n")

type ResponseWriter interface {
	io.Closer
	http.ResponseWriter
	Key() multihash.Multihash
	PathType() string
}

func New(w http.ResponseWriter, r *http.Request, preferJson bool) (ResponseWriter, error) {
	jsonW, err := newJsonResponseWriter(w, r, preferJson)
	if err != nil {
		return nil, err
	}

	var b []byte
	pathType := path.Base(path.Dir(r.URL.Path))
	fmt.Println("--->", pathType, "=", path.Base(r.URL.Path))
	switch pathType {
	case "multihash":
		b, err = base58.Decode(strings.TrimSpace(path.Base(r.URL.Path)))
		if err != nil {
			return nil, apierror.New(multihash.ErrInvalidMultihash, http.StatusBadRequest)
		}
	case "cid":
		c, err := cid.Decode(strings.TrimSpace(path.Base(r.URL.Path)))
		if err != nil {
			return nil, apierror.New(err, http.StatusBadRequest)
		}
		b = c.Hash()
	default:
		return nil, apierror.New(errors.New("unsupported resource type"), http.StatusBadRequest)
	}

	dm, err := multihash.Decode(b)
	if err != nil {
		return nil, apierror.New(err, http.StatusBadRequest)
	}

	var rspW ResponseWriter

	mh := multihash.Multihash(b)
	if dm.Code == multihash.DBL_SHA2_256 {
		encW := &EncResponseWriter{
			jsonResponseWriter: jsonW,
			pathType:           pathType,
		}
		encW.encResult.Multihash = mh
		rspW = encW
	} else {
		plainW := &PlainResponseWriter{
			jsonResponseWriter: jsonW,
			pathType:           pathType,
		}
		plainW.result.Multihash = mh
		rspW = plainW
	}

	return rspW, nil
}
