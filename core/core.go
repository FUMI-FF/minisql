// Package core is ..
package core

import (
	"errors"
	"fmt"
	"minisql/backend"
	"strconv"
	"strings"
)

type StatementType = int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Statement struct {
	_type       StatementType
	rowToInsert backend.Row
}

var (
	ErrPrepareStmtInvalidSyntax    = errors.New("invalid syntax")
	ErrPrepareStmtUnrecognizedStmt = errors.New("unrecognized statement")
	ErrPrepareStmtStringTooLong    = errors.New("string is too long")
	ErrPrepareStmtNegativeID       = errors.New("ID must be positive")
)

func PrepareStatement(input string) (*Statement, error) {
	stmt := Statement{}
	if strings.HasPrefix(input, "insert") {
		stmt._type = StatementInsert

		s := strings.Split(input, " ")
		if len(s) != 4 {
			return nil, ErrPrepareStmtInvalidSyntax
		}
		id, err := strconv.Atoi(s[1])
		if err != nil {
			return nil, err
		}
		if id < 0 {
			return nil, ErrPrepareStmtNegativeID
		}
		username := []byte(s[2])
		if len(username) > backend.UsernameSize {
			return nil, ErrPrepareStmtStringTooLong
		}
		email := []byte(s[3])
		if len(email) > backend.EmailSize {
			return nil, ErrPrepareStmtStringTooLong
		}
		stmt.rowToInsert.ID = uint32(id)
		copy(stmt.rowToInsert.Username[:], username)
		copy(stmt.rowToInsert.Email[:], email)
		return &stmt, nil
	}

	if strings.HasPrefix(input, "select") {
		stmt._type = StatementSelect
		return &stmt, nil
	}
	return nil, ErrPrepareStmtUnrecognizedStmt
}

var (
	ErrExecuteStmtTableFull       = errors.New("table is full")
	ErrExecuteStmtInvalidStmtType = errors.New("invalid statement type")
)

func ExecuteStatement(stmt *Statement, db *backend.DB) error {
	switch stmt._type {
	case StatementInsert:
		return executeInsert(stmt, db)
	case StatementSelect:
		return executeSelect(stmt, db)
	}
	return nil
}

func executeInsert(stmt *Statement, db *backend.DB) error {
	if stmt._type != StatementInsert {
		return ErrExecuteStmtInvalidStmtType
	}
	return db.Insert(&stmt.rowToInsert)
}

func executeSelect(stmt *Statement, db *backend.DB) error {
	if stmt._type != StatementSelect {
		return ErrExecuteStmtInvalidStmtType
	}

	rows, err := db.SelectAll()
	if err != nil {
		return err
	}

	for _, r := range rows {
		fmt.Printf("%s\n", r)
	}

	return nil
}
