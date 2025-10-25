// Package backend ...
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

func (table *Table) Close() error {
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
