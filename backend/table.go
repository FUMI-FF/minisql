package backend

import "fmt"

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
