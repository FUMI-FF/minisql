package backend

import (
	"errors"
	"os"
)

type Cursor struct {
	table   *Table
	pageNum uint32
	cellNum uint32
}

func tableStart(table *Table) *Cursor {
	return &Cursor{
		table:   table,
		pageNum: table.rootPageNum,
		cellNum: 0,
	}
}

func tableEnd(table *Table) *Cursor {
	root, err := table.pager.getPage(table.rootPageNum)
	if err != nil {
		os.Exit(1)
	}
	node := NewLeafNode(root)

	return &Cursor{
		table:   table,
		pageNum: table.rootPageNum,
		cellNum: node.NumCells(),
	}
}

// indicate if current position is a last row
func (c *Cursor) end() (bool, error) {
	root, err := c.table.pager.getPage(c.table.rootPageNum)
	if err != nil {
		return false, err
	}
	node := NewLeafNode(root)
	return c.cellNum >= node.NumCells(), nil
}

func (c *Cursor) value() (row []byte, err error) {
	page, err := c.table.pager.getPage(c.pageNum)
	if err != nil {
		return nil, err
	}
	node := NewLeafNode(page)
	return node.Value(c.cellNum), nil
}

func (c *Cursor) advance() {
	c.cellNum += 1
}

func (c *Cursor) read() (*Row, error) {
	isend, err := c.end()
	if err != nil {
		return nil, err
	}
	if isend {
		return nil, nil
	}
	buf, err := c.value()
	if err != nil {
		return nil, err
	}
	r, _ := deserializeRow(buf, 0)
	return r, nil
}

func (c *Cursor) write(r *Row) error {
	// allow rowNum == numRows (append)
	page, err := c.table.pager.getPage(c.pageNum)
	if err != nil {
		return err
	}
	node := NewLeafNode(page)
	
	c.cellNum += 1
	if c.cellNum > node.NumCells() {
		return errors.New("invalid cursor position")
	}

	buf, err := c.value()
	if err != nil {
		return err
	}

	serializeRow(buf, offset, r)

	if c.rowNum == c.table.numRows {
		c.table.numRows += 1
	}
	return nil
}
