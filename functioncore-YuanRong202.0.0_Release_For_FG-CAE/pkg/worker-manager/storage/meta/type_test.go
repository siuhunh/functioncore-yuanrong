package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateNodeLabel(t *testing.T) {
	l := StorageList{
		ListMeta: ListMeta{
			SelfLink:        "aa",
			ResourceVersion: "bb",
			Continue:        "tr",
		},
	}
	data, _ := l.Data()
	assert.NotEmpty(t, data)
}
