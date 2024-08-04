package errs

import (
	"errors"
	"fmt"
)

var NotFoundError = errors.New("not found")
var AlreadyExistsError = fmt.Errorf("item already exists")
