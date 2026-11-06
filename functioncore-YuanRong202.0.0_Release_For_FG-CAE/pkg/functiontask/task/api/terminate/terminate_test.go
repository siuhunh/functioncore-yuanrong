package terminate

import (
	"testing"

	"gotest.tools/assert"
)

func TestIsReleaseBridge(t *testing.T) {
	req := &RequestPb{}
	flag, _ := IsRelease(nil, req)
	assert.Equal(t, flag, true)
}
