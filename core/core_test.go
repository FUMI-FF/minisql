package core

import (
	"minisql/backend"
	"testing"

	"github.com/stretchr/testify/assert"
)

func FixedBytes32(s string) [32]byte {
	var b [32]byte
	copy(b[:], s)
	return b
}

func FixedBytes255(s string) [255]byte {
	var b [255]byte
	copy(b[:], s)
	return b
}

func TestPrepareStatemet(t *testing.T) {
	tests := []struct {
		input    string
		expected *Statement
	}{
		{
			input: "insert 1 test foo@bar",
			expected: &Statement{
				_type: StatementInsert,
				rowToInsert: backend.Row{
					ID:       1,
					Username: FixedBytes32("test"),
					Email:    FixedBytes255("foo@bar"),
				},
			},
		},
		{
			input: "select",
			expected: &Statement{
				_type: StatementSelect,
			},
		},
	}

	for _, tt := range tests {
		stmt, err := PrepareStatement(tt.input)
		assert.NoError(t, err)
		assert.Equal(t, tt.expected, stmt)
	}
}

func TestExecuteStatemet(t *testing.T) {
	tests := []struct {
		stmt *Statement
		err  error
	}{
		{
			stmt: &Statement{
				_type: StatementInsert,
				rowToInsert: backend.Row{
					ID:       1,
					Username: FixedBytes32("test"),
					Email:    FixedBytes255("foo@bar"),
				},
			},
		},
		{
			stmt: &Statement{_type: StatementSelect},
			err:  nil,
		},
	}

	for _, tt := range tests {
		table, err := backend.OpenDB("test.db")
		defer func() {
			err = table.Close()
			assert.NoError(t, err)
		}()
		assert.NoError(t, err)
		err = ExecuteStatement(tt.stmt, table)
		assert.NoError(t, err)
	}
}
