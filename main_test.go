package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func username(s string) [32]byte {
	var arr [32]byte
	copy(arr[:], s)
	return arr
}

func email(s string) [255]byte {
	var arr [255]byte
	copy(arr[:], s)
	return arr
}

func Test_prepareStatement(t *testing.T) {
	tests := []struct {
		input    string
		expected *Statement
		err      error
	}{
		{
			input: "insert 1 test foo@bar.com",
			expected: &Statement{
				_type: StatementInsert,
				rowToInsert: Row{
					id:       1,
					username: username("test"),
					email:    email("foo@bar.com"),
				},
			},
			err: nil,
		},
		{
			input: fmt.Sprintf("insert 1 %s foo@bar.com", strings.Repeat("a", 32)),
			expected: &Statement{
				_type: StatementInsert,
				rowToInsert: Row{
					id:       1,
					username: username(strings.Repeat("a", 32)),
					email:    email("foo@bar.com"),
				},
			},
			err: nil,
		},
		{
			input: fmt.Sprintf("insert 1 test %s@x.com", strings.Repeat("a", 255-len("@x.com"))),
			expected: &Statement{
				_type: StatementInsert,
				rowToInsert: Row{
					id:       1,
					username: username("test"),
					email:    email(fmt.Sprintf("%s@x.com", strings.Repeat("a", 255-len("@x.com")))),
				},
			},
			err: nil,
		},
		{
			input: "select",
			expected: &Statement{
				_type: StatementSelect,
			},
			err: nil,
		},
		{
			input:    "insert -1 test foo@bar.com",
			expected: nil,
			err:      ErrPrepareStmtNegativeID,
		},
		{
			input:    fmt.Sprintf("insert 1 %s foo@bar.com", strings.Repeat("a", 33)),
			expected: nil,
			err:      ErrPrepareStmtStringTooLong,
		},
		{
			input:    fmt.Sprintf("insert 1 test %s@x.com", strings.Repeat("a", 256-len("@x.com"))),
			expected: nil,
			err:      ErrPrepareStmtStringTooLong,
		},
	}

	for _, tt := range tests {
		actual, err := prepareStatement(tt.input)
		assert.Equal(t, tt.expected, actual)
		assert.Equal(t, tt.err, err)
	}
}

func Test_executeStatement(t *testing.T) {
	// arrange
	insertStmt := Statement{
		_type: StatementInsert,
		rowToInsert: Row{
			id:       1,
			username: username("test"),
			email:    email("foo@bar.com"),
		},
	}
	selectStmt := Statement{
		_type: StatementSelect,
	}
	table := Table{}

	// act & assert
	err := executeStatement(&insertStmt, &table)
	assert.NoError(t, err)
	err = executeSelect(&selectStmt, &table)
	assert.NoError(t, err)
}

func Test_executeStatement_with_table_full(t *testing.T) {
	table := Table{}
	for i := 0; i < 1400; i++ {
		executeStatement(&Statement{
			_type: StatementInsert,
			rowToInsert: Row{
				id:       uint32(i+1),
				username: username("test"),
				email:    email("foo@bar.com"),
			},
		}, &table)
	}

	// act
	err := executeStatement(&Statement{
		_type: StatementInsert,
		rowToInsert: Row{
			id: 1401,
			username: username("test"),
			email: email("foo@bar.com"),
		},
	}, &table)

	// assert
	assert.Error(t, err, ErrExecuteStmtTableFull)
}
