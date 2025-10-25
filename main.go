package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
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

type Pager struct {
	file       *os.File
	fileLength uint32
	pages      [TableMaxPages][]byte
}

type Table struct {
	numRows uint32
	pager   *Pager
}

func pagerOpen(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, syscall.S_IWUSR|syscall.S_IRUSR)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &Pager{
		file:       file,
		fileLength: uint32(info.Size()),
	}, nil
}

func (p *Pager) getPage(pageNum uint32) ([]byte, error) {
	if pageNum > TableMaxPages {
		return nil, errors.New("tried to fetch page number out of bound")
	}
	if p.pages[pageNum] == nil {
		// cache miss. allocate memory and load file
		page := make([]byte, PageSize)
		numPages := p.fileLength / PageSize

		// we might save a partial page at the end of the file
		if (p.fileLength % PageSize) > 0 {
			numPages += 1
		}

		if pageNum < numPages {
			_, err := p.file.ReadAt(page, int64(pageNum)*PageSize)
			if errors.Is(err, io.EOF) {
				return page, nil
			} else if err != nil {
				return nil, err
			}
		}

		p.pages[pageNum] = page
	}
	return p.pages[pageNum], nil
}

func (p *Pager) flush(pageNum uint32, bytesToWrite uint32) error {
	if p.pages[pageNum] == nil {
		return errors.New("tried to flush null page")
	}
	if bytesToWrite == 0 || bytesToWrite > PageSize {
		return errors.New("invalid bytesToWrite")
	}

	offset := int64(pageNum) * int64(PageSize)
	_, err := p.file.WriteAt(p.pages[pageNum][:bytesToWrite], offset)
	if err != nil {
		return err
	}

	return nil
}

func dbOpen(filename string) (*Table, error) {
	pager, err := pagerOpen(filename)
	if err != nil {
		return nil, err
	}
	numRows := pager.fileLength / RowSize
	return &Table{
		numRows: numRows,
		pager:   pager,
	}, nil
}

func dbClose(table *Table) error {
	numFullPages := table.numRows / RowsPerPage

	for i := 0; i < int(numFullPages); i++ {
		if table.pager.pages[i] == nil {
			continue
		}
		err := table.pager.flush(uint32(i), PageSize)
		if err != nil {
			return err
		}
		table.pager.pages[i] = nil
	}

	// flush partial(tail) page
	numAdditionalRows := table.numRows % RowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if table.pager.pages[pageNum] != nil {
			bytes := numAdditionalRows * RowSize
			err := table.pager.flush(pageNum, bytes)
			if err != nil {
				return err
			}
			table.pager.pages[pageNum] = nil
		}
	}
	
	// ensure the content flushed to disk
	if err := table.pager.file.Sync(); err != nil {
		return err
	}

	return table.pager.file.Close()
}

func (t *Table) Serialize(r *Row) error {
	if t.numRows >= TableMaxRows {
		return fmt.Errorf("table full: %d rows (max %d)", t.numRows, TableMaxPages)
	}

	pageNum := t.numRows / RowsPerPage
	if pageNum > TableMaxPages {
		return fmt.Errorf("table index out of range: %d", pageNum)
	}

	page, err := t.pager.getPage(pageNum)
	if err != nil {
		return err
	}

	rowOffset := t.numRows % RowsPerPage
	byteOffset := rowOffset * RowSize
	serializeRow(page, int(byteOffset), r)

	t.pager.pages[pageNum] = page

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

	page, err := t.pager.getPage(uint32(pageNum))
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, fmt.Errorf("page %d not allocated", pageNum)
	}
	r, _ := deserializeRow(page, byteOffset)
	return r, nil
}

var (
	ErrUnrecognizedMetaCmd = errors.New("unrecognized meta command")
)

func doMetaCommand(input string, table *Table) error {
	s := strings.TrimSpace(input)
	if s == ".exit" {
		dbClose(table)
		os.Exit(0)
	}
	return ErrUnrecognizedMetaCmd
}

var (
	ErrPrepareStmtInvalidSyntax    = errors.New("syntax error")
	ErrPrepareStmtUnrecognizedStmt = errors.New("unrecognized statement")
	ErrPrepareStmtStringTooLong    = errors.New("string is too long")
	ErrPrepareStmtNegativeID       = errors.New("ID must be positive")
)

func prepareStatement(input string) (*Statement, error) {
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
		if len(username) > UsernameSize {
			return nil, ErrPrepareStmtStringTooLong
		}
		email := []byte(s[3])
		if len(email) > EmailSize {
			return nil, ErrPrepareStmtStringTooLong
		}
		stmt.rowToInsert.id = uint32(id)
		copy(stmt.rowToInsert.username[:], username)
		copy(stmt.rowToInsert.email[:], email)
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
		return ErrExecuteStmtTableFull
	}
	if stmt._type != StatementInsert {
		return ErrExecuteStmtInvalidStmtType
	}
	return table.Serialize(&stmt.rowToInsert)
}

func executeSelect(stmt *Statement, table *Table) error {
	if stmt._type != StatementSelect {
		return ErrExecuteStmtInvalidStmtType
	}
	for i := 0; i < int(table.numRows); i++ {
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
	if len(os.Args) < 2 {
		fmt.Println("Must apply a database filename")
		os.Exit(1)
	}

	table, err := dbOpen(os.Args[1])
	if err != nil {
		fmt.Println("Failed to open database")
		os.Exit(1)
	}
	defer dbClose(table)

	reader := bufio.NewReader(os.Stdin)

	for {
		printPrompt()
		input := readInput(reader)
		if strings.HasPrefix(input, ".") {
			if err := doMetaCommand(input, table); err != nil {
				fmt.Printf("Failed to execute meta command: %s\n", err)
			}
			continue
		}

		stmt, err := prepareStatement(input)
		if err != nil {
			fmt.Printf("Failed to prepare statement: %s\n", err)
			continue
		}

		err = executeStatement(stmt, table)
		if err != nil {
			fmt.Printf("`executeStatement` failed: %s\n", err)
		}
		fmt.Println("Executed")
	}
}
