package backend

import (
	"encoding/binary"
	"fmt"
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
	ID       uint32
	Username [UsernameSize]byte
	Email    [EmailSize]byte
}

func (r Row) String() string {
	return fmt.Sprintf("(%d, %s, %s)", r.ID, string(r.Username[:]), string(r.Email[:]))
}

const (
	PageSize      = 4096
	TableMaxPages = 100
	RowsPerPage   = PageSize / RowSize
	TableMaxRows  = RowsPerPage * TableMaxPages
)

func OpenDB(filename string) (*Table, error) {
	pager, err := newPage(filename)
	if err != nil {
		return nil, err
	}
	numRows := pager.fileLength / RowSize
	return &Table{numRows: numRows, pager: pager}, nil
}

func (t *Table) Close() error {
	numFullPages := t.numRows / RowsPerPage

	for i := 0; i < int(numFullPages); i++ {
		if t.pager.pages[i] == nil {
			continue
		}
		err := t.pager.flush(uint32(i), PageSize)
		if err != nil {
			return err
		}
		t.pager.pages[i] = nil
	}

	// flush partial(tail) page
	numAdditionalRows := t.numRows % RowsPerPage
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if t.pager.pages[pageNum] != nil {
			bytes := numAdditionalRows * RowSize
			err := t.pager.flush(pageNum, bytes)
			if err != nil {
				return err
			}
			t.pager.pages[pageNum] = nil
		}
	}

	// ensure the content flushed to disk
	if err := t.pager.file.Sync(); err != nil {
		return err
	}

	return t.pager.file.Close()
}

type Table struct {
	numRows uint32
	pager   *Pager
}

func (t *Table) NumOfRows() uint32 {
	return t.numRows
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

func (t *Table) Deserialize(rowIdx int) (*Row, error) {
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

func serializeRow(buf []byte, offset int, row *Row) int {
	// ID
	binary.LittleEndian.PutUint32(buf[offset:], row.ID)
	offset += 4
	// Username
	copy(buf[offset:offset+UsernameSize], row.Username[:])
	offset += UsernameSize
	// Email
	copy(buf[offset:offset+EmailSize], row.Email[:])
	offset += EmailSize
	return offset
}

func deserializeRow(data []byte, offset int) (*Row, int) {
	var r Row
	// ID
	r.ID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4
	// Username
	copy(r.Username[:], data[offset:offset+UsernameSize])
	offset += UsernameSize
	// Email
	copy(r.Email[:], data[offset:offset+EmailSize])
	offset += EmailSize
	return &r, offset
}
