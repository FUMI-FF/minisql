package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
)

type StatementType = int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

const (
	IDSize         = 4
	UsernameSize   = 32
	EmailSize      = 255
	RowSize        = IDSize + UsernameSize + EmailSize
	IDOffset       = 0
	UsernameOffset = IDOffset + IDSize
	EmalOffset     = UsernameOffset + UsernameSize
)

type Row struct {
	id       uint32
	username [UsernameSize]byte
	email    [EmailSize]byte
}

func (r Row) String() string {
	return fmt.Sprintf("(%d, %s, %s)", r.id, string(r.username[:]), string(r.email[:]))
}

type Statement struct {
	_type       StatementType
	rowToInsert Row
}

func serializeRow(buf []byte, offset int, row *Row) int {
	// ID
	binary.LittleEndian.PutUint32(buf[offset:], row.id)
	offset += 4
	// Username
	copy(buf[offset:offset+UsernameSize], row.username[:])
	offset += UsernameSize
	// Email
	copy(buf[offset:offset+EmailSize], row.email[:])
	offset += EmailSize
	return offset
}

func deserializeRow(data []byte, offset int) (*Row, int) {
	var r Row
	// ID
	r.id = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	// Username
	copy(r.username[:], data[offset:offset+UsernameSize])
	offset += UsernameSize
	// Email
	copy(r.email[:], data[offset:offset+EmailSize])
	offset += EmailSize
	return &r, offset
}

const (
	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

type Table struct {
	numRows uint32 // 
	pages [TableMaxPages][]byte
}

func (t *Table) Serialize(r *Row) error {
	if t.numRows >=  TableMaxRows {
		return fmt.Errorf("table full: %d rows (max %d)", t.numRows, TableMaxPages)
	}

	pageNum :=  t.numRows / RowsPerPage
	
	if pageNum > TableMaxPages {
		return fmt.Errorf("table index out of range: %d", pageNum)
	}

	if t.pages[pageNum] == nil {
		t.pages[pageNum] = make([]byte, PageSize)
	}

	rowOffset := t.numRows % RowsPerPage
	byteOffset := rowOffset * RowSize
	
	serializeRow(t.pages[pageNum],int(byteOffset), r)

	t.numRows += 1

	return nil
}

func (t *Table) deserialize(rowIdx int) (*Row, error) {
	if rowIdx > int(t.numRows) {
		return nil, fmt.Errorf("row %d not written (rowNum=%d)", rowIdx, t.numRows)
	}
	pageNum := rowIdx / RowsPerPage
	rowInPage := rowIdx % RowsPerPage
	byteOffset := rowInPage * RowSize

	page := t.pages[pageNum]
	if page == nil {
		return nil, fmt.Errorf("page %d not allocated", pageNum)
	}
	r, _ := deserializeRow(page, byteOffset)
	return r, nil
}

var (
	ErrUnrecognizedMetaCmd = errors.New("unrecognized meta command")
)

func doMetaCommand(input string) error {
	s := strings.TrimSpace(input)
	if s == ".exit" {
		os.Exit(0)
		return nil
	}
	return ErrUnrecognizedMetaCmd
}

var (
	ErrPrepareStmtInvalidSyntax = errors.New("syntax error")
	ErrPrepareStmtUnrecognizedStmt = errors.New("unrecognized statement")
)

func prepareStatement(input string) (*Statement, error) {
	stmt := Statement{}
	if strings.HasPrefix(input, "insert") {
		stmt._type = StatementInsert
		var id uint32
		var username, email string
		n, err := fmt.Sscanf(input, "insert %d %s %s", &id, &username, &email)
		if err != nil {
			return nil, err
		}
		if n < 3 {
			return nil, ErrPrepareStmtInvalidSyntax
		}
		stmt.rowToInsert.id = id
		copy(stmt.rowToInsert.username[:], []byte(username))
		copy(stmt.rowToInsert.email[:], []byte(email))
		return &stmt, nil
	}
	if strings.HasPrefix(input, "select") {
		stmt._type = StatementSelect
		return &stmt, nil
	}
	return nil, ErrPrepareStmtUnrecognizedStmt
}

func executeStatement(stmt *Statement, table *Table) error {
	switch stmt._type {
	case StatementInsert:
		return executeInsert(stmt, table)
	case StatementSelect:
		return executeSelect(stmt, table)
	}
	return nil
}

func executeInsert(stmt *Statement, table *Table) error {
	if table.numRows >= TableMaxRows {
		return fmt.Errorf("table is full")
	}
	if stmt._type != StatementInsert {
		return fmt.Errorf("StatementType is not StatementInsert")
	}
	return table.Serialize(&stmt.rowToInsert)
}

func executeSelect(stmt *Statement, table *Table) error {
	if stmt._type != StatementSelect {
		return fmt.Errorf("StatementType is not StatementSelect")
	}
	for i:= 0; i < int(table.numRows); i++ {
		r, err := table.deserialize(i)
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", r)
	}
	return nil
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) string {
	input, err := reader.ReadString('\n')
	if err != nil {
		os.Exit(1)
	}
	return strings.TrimSpace(input)
}

func main() {
	table := Table{}
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		input := readInput(reader)
		if strings.HasPrefix(input, ".") {
			if err := doMetaCommand(input); err != nil {
				fmt.Printf("Failed to execute meta command: %s", err)
			}
			continue
		}

		stmt, err := prepareStatement(input)
		if err != nil {
			fmt.Printf("Failed to prepare statement: %s", err)
			continue
		}

		executeStatement(stmt, &table)
		fmt.Println("Executed")
	}
}
