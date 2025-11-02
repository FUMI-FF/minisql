package backend

import "errors"

type Cursor struct {
	table  *Table
	rowNum uint32
}

func tableStart(table *Table) *Cursor {
	return &Cursor{
		table:  table,
		rowNum: 0,
	}
}

func tableEnd(table *Table) *Cursor {
	return &Cursor{
		table:  table,
		rowNum: table.numRows,
	}
}

// indicate if current position is a last row
func (c *Cursor) end() bool {
	return c.rowNum >= c.table.numRows
}

func (c *Cursor) value() (page []byte, offset uint32, err error) {
	if c.rowNum > c.table.numRows {
		return nil, 0, errors.New("cursor beyond end")
	}

	pageNum := c.rowNum / RowsPerPage
	page, err = c.table.pager.getPage(pageNum)
	if err != nil {
		return nil, 0, err
	}
	rowOffset := c.rowNum % RowsPerPage
	byteOffset := rowOffset * RowSize
	return page, byteOffset, nil
}

func (c *Cursor) advance() {
	c.rowNum += 1
}

func (c *Cursor) read() (*Row, error) {
	if c.end() {
		return nil, nil
	}
	buf, offset, err := c.value()
	if err != nil {
		return nil, err
	}
	r, _ := deserializeRow(buf, int(offset))
	return r, nil
}

func (c *Cursor) write(r *Row) error {
	// allow rowNum == numRows (append)
	if c.rowNum > c.table.numRows {
		return errors.New("invalid cursor position")
	}

	buf, offset, err := c.value()
	if err != nil {
		return err
	}
	serializeRow(buf, offset, r)

	if c.rowNum == c.table.numRows {
		c.table.numRows += 1
	}
	return nil
}
