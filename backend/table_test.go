package backend

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	// arrage
	table, err := OpenDB("test.db")
	require.NoError(t, err)
	defer table.Close()
	defer os.Remove("test.db")

	var username [32]byte
	copy(username[:], []byte("test"))

	var email [255]byte
	copy(email[:], []byte("foo@bar"))

	// act
	err = table.Serialize(&Row{ID: 1, Username: username, Email: email})
	assert.NoError(t, err)
	r, err := table.Deserialize(0)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, r.ID, uint32(1))
	assert.Equal(t, r.Username, username)
	assert.Equal(t, r.Email, email)
}
