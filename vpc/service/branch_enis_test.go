package service

import (
	"github.com/pkg/errors"
	"gotest.tools/assert"
	"testing"
)

func TestPersistentError(t *testing.T) {
	err := errors.New("test error")
	assert.Assert(t, !errors.Is(err, &persistentError{}))
	err2 := &persistentError{err:err}
	assert.Assert(t, errors.Is(err2, &persistentError{}))

}
