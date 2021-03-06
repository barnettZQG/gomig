package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/barnettzqg/gomig/db/common"
	"github.com/lib/pq"
)

var (
	PG_DB_EXECUTOR_VERBOSE = true
)

type PgDbExecutor struct {
	common.DbExecutor
	bulkStmt *sql.Stmt
}

// errfn turns generic errors into more informational ones if possible
func errfn(err error) error {
	switch err := err.(type) {
	case *pq.Error:
		return fmt.Errorf("Error %v\nMESSAGE: %s\nDETAIL: %s\nWHERE: %s",
			err.Code, err.Message, err.Detail, err.Where)
	default:
		return err
	}
}

func NewPgDbExecutor(db *sql.DB) (*PgDbExecutor, error) {
	base, err := common.NewDbExecutor(db, errfn)
	if err != nil {
		return nil, err
	}

	return &PgDbExecutor{*base, nil}, nil
}

func (e *PgDbExecutor) BulkInit(table string, columns ...string) error {
	db := e.GetDb()
	if db == nil {
		return errors.New("executor did not have a valid database")
	}

	var (
		stmt *sql.Stmt
		err  error
	)
	tx := e.GetTx()
	copySql := pq.CopyIn(table, columns...)
	if tx == nil {
		stmt, err = db.Prepare(copySql)
	} else {
		stmt, err = tx.Prepare(copySql)
	}
	if err != nil {
		return err
	}
	e.bulkStmt = stmt

	return nil
}

func (e *PgDbExecutor) BulkAddRecord(args ...interface{}) error {
	/* TODO: does not check if bulkStmt exists yet */
	_, err := e.bulkStmt.Exec(args...)
	return err
}

func (e *PgDbExecutor) BulkFinish() (err error) {
	stmt := e.bulkStmt
	defer func() {
		cerr := stmt.Close()
		if err == nil && cerr != nil {
			log.Println("pg_executor: could not properly close bulk statement", cerr)
			err = cerr
		}

		/* make sure to reset the bulk statement */
		e.bulkStmt = nil
	}()

	_, err = stmt.Exec()
	return
}

func (e *PgDbExecutor) HasCapability(capability int) bool {
	return false
	//return capability == common.CapBulkTransfer
}
