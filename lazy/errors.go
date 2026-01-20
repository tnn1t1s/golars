package lazy

import "errors"

var (
	errInvalidChildren = errors.New("invalid number of children")
	errMissingSchema   = errors.New("missing schema")
	errMissingInput    = errors.New("missing input")
	errMissingSource   = errors.New("missing data source")
)
