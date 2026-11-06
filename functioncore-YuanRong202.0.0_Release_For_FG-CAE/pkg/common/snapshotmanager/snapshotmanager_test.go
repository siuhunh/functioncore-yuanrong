package snapshotmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSnapshotManagerMode(t *testing.T) {
	SetSnapshotManagerMode(true)
	assert.Equal(t, true, GetSnapshotManagerMode())
	SetSnapshotManagerMode(false)
}
