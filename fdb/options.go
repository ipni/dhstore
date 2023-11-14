//go:build fdb

package fdb

type (
	Option  func(*options) error
	options struct {
		clusterFile string
		apiVersion  int
	}
)

func newOptions(o ...Option) (*options, error) {
	var opts options
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	return &opts, nil
}

func WithApiVersion(v int) Option {
	return func(o *options) error {
		o.apiVersion = v
		return nil
	}
}

func WithClusterFile(f string) Option {
	return func(o *options) error {
		o.clusterFile = f
		return nil
	}
}
