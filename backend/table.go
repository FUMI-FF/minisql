package backend

import (
	"encoding/binary"
	"fmt"
)

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
