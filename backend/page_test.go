package backend

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPage_WithPageCache(t *testing.T) {
	// arrange
	pager, err := newPage("test.db")
	require.NoError(t, err)
	defer pager.file.Close()
	pager.pages[0] = []byte("test")
	
	// act
	b, err := pager.getPage(0)
	
	// assert
	assert.NoError(t, err)
	assert.Equal(t, []byte("test"), b)
}

func TestGetPage_WithoutPageCache(t *testing.T) {
	// arrange
	pager, err := newPage("test.db")
	require.NoError(t, err)
	defer pager.file.Close()

	// act
	b, err := pager.getPage(0)

	// assert
	assert.NoError(t, err)
	assert.Equal(t, make([]byte, PageSize), b)
	os.Remove("test.db")
}

func TestFlush(t *testing.T) {
	// arrange
	pager, err := newPage("test.db")
	require.NoError(t, err)
	defer pager.file.Close()
	defer os.Remove("test.db")
	pager.pages[0] = []byte("test")

	// act
	err = pager.flush(0, uint32(len(pager.pages[0])))

	// assert
	assert.NoError(t, err)
}

func TestFlush_NullPage(t *testing.T){
	// arrange
	pager, err := newPage("test.db")
	require.NoError(t, err)
	defer pager.file.Close()
	defer os.Remove("test.db")

	// act
	err = pager.flush(0, 0)

	// assert
	assert.ErrorIs(t, err, ErrPagerNullPageFlush)
}

func TestFlush_InvalidSize(t *testing.T) {
	// arrange
	tests := []uint32{0, PageSize}
	
	pager, err := newPage("test.db")
	require.NoError(t, err)
	defer pager.file.Close()
	defer os.Remove("test.db")
	pager.pages[0] = []byte("test")

	for _, tt := range tests {
		// act
		err = pager.flush(0, tt)

		// assert
		assert.ErrorIs(t, err, ErrPagerInvalidSizeFlush)
	}
}
