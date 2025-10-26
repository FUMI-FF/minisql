// Package backend ...
package backend

import (
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
