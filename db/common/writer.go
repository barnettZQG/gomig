package common

import (
	"database/sql"
	"io"
)

type Writer interface {
	/*
		CreateTable(t *Table) error
		Truncate(t *Table) error
	*/

	/* merge the contents of table */
	MergeTable(src *Table, dstName, extraDstCond string, r Reader) error
	ClearTable([]string)
	GetDB() *sql.DB

	/* (over)write the contents of table */
	/* WriteTable(t *Table) error */

	/*
		CreateIndices(t *Table) error
		CreateConstraints(t *Table) error
	*/
}

type WriteCloser interface {
	io.Closer
	Writer
}
