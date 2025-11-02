package backend

import "errors"

type DB struct {
	table *Table
}

func Open(filename string) (*DB, error) {
	pager, err := newPage(filename)
	if err != nil {
		return nil, err
	}
	return &DB{
		table: newTable(pager),
	}, nil
}

func (db *DB) Close() error {
	return db.table.Close()
}

func (db *DB) Insert(r *Row) error {
	if db.table.numRows >= TableMaxRows {
		return errors.New("table full")
	}
	cur := tableEnd(db.table)
	return cur.write(r)
}

func (db *DB) SelectAll() ([]*Row, error) {
	rows := []*Row{}
	cur := tableStart(db.table)
	for !cur.end() {
		r, err := cur.read()
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
		cur.advance()
	}
	return rows, nil
}
