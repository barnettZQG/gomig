package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/barnettzqg/gomig/db/common"
)

var PG_W_VERBOSE = true

var (
	postgresInit = []string{
		"SET client_encoding = 'UTF8';",
		"SET STANDARD_CONFORMING_STRINGS = on;",
		//"SET check_function_bodies = false;",
		"SET client_min_messages = warning;",
	}
)

const (
	explainQuery = `
SELECT col.column_name AS field,
       CASE
        WHEN col.character_maximum_length IS NOT NULL THEN col.data_type || '(' || col.character_maximum_length || ')'
        ELSE col.data_type
       END AS type,
       col.is_nullable AS null,
       CASE
        WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PRI'
        ELSE ''
       END AS key,
       '' AS default,
       '' AS extra
       --kcu.constraint_name AS constraint_name
       --kcu.*,
       --tc.*
FROM   information_schema.columns col
LEFT JOIN   information_schema.key_column_usage kcu ON (kcu.table_name = col.table_name AND kcu.column_name = col.column_name)
LEFT JOIN   information_schema.table_constraints AS tc ON (kcu.constraint_name = tc.constraint_name)
WHERE  col.table_name = '%v'
ORDER BY col.ordinal_position;`
)

type genericPostgresWriter struct {
	e               common.Executor
	insertBulkLimit int
}

func (w *genericPostgresWriter) bulkTransfer(src *common.Table, dstName string, rows *sql.Rows) (err error) {
	ex := w.e

	colnames := make([]string, 0, len(src.Columns))
	for _, col := range src.Columns {
		colnames = append(colnames, col.Name)
	}

	if err = ex.BulkInit(dstName, colnames...); err != nil {
		return
	}
	defer func() {
		berr := ex.BulkFinish()
		if err == nil {
			/* if there was no earlier error, set the one from BulkFinish */
			err = berr
		}
	}()

	/* create a slice with the right types to extract into, and let the SQL
	 * driver take care of the conversion */
	vals := NewTypedSlice(src)

	for rows.Next() {
		if err = rows.Scan(vals...); err != nil {
			return fmt.Errorf("postgres: error while reading from source: %v", err)
		}

		if err = ex.BulkAddRecord(vals...); err != nil {
			return fmt.Errorf("postgres: error during bulk insert: %v", err)
		}
	}

	return
}

func (w *genericPostgresWriter) normalTransfer(src *common.Table, dstName string, rows *sql.Rows) error {
	/* an alternate way to do this, with type assertions
	 * but possibly less accurately: http://go-database-sql.org/varcols.html */
	pointers := make([]interface{}, len(src.Columns))
	containers := make([]sql.RawBytes, len(src.Columns))
	for i, _ := range pointers {
		pointers[i] = &containers[i]
	}
	stringrep := make([]string, 0, len(src.Columns))
	insertLines := make([]string, 0, 32)

	var columns string
	for _, idx := range src.Columns {
		if columns == "" {
			columns += "\"" + idx.Name + "\""
		} else {
			columns += ",\"" + idx.Name + "\""
		}
	}
	for rows.Next() {
		err := rows.Scan(pointers...)
		if err != nil {
			log.Println("postgres: error while reading from source:", err)
			return err
		}
		for idx, val := range containers {
			str, err := RawToPostgres(val, src.Columns[idx].Type)
			if err != nil {
				return err
			}
			stringrep = append(stringrep, str)
		}

		insertLines = append(insertLines, "("+strings.Join(stringrep, ",")+")")
		stringrep = stringrep[:0]

		if len(insertLines) >= w.insertBulkLimit {
			err = w.e.Submit(fmt.Sprintf("INSERT INTO %v(%s) VALUES\n\t%v;\n",
				dstName, columns, strings.Join(insertLines, ",\n\t")))
			if err != nil {
				return err
			}
			insertLines = insertLines[:0]
		}
	}

	if len(insertLines) > 0 {
		err := w.e.Submit(fmt.Sprintf("INSERT INTO %v(%s) VALUES\n\t%v;\n",
			dstName, columns, strings.Join(insertLines, ",\n\t")))
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *genericPostgresWriter) transferTable(src *common.Table, dstName string, r common.Reader) error {
	/* bulk insert values */
	rows, err := r.Read(src)
	if err != nil {
		return err
	}
	defer rows.Close()

	if PG_W_VERBOSE {
		log.Print("postgres: query done, scanning rows...")
	}
	fmt.Println(w.e.HasCapability(common.CapBulkTransfer))
	if w.e.HasCapability(common.CapBulkTransfer) {
		if PG_W_VERBOSE {
			log.Print("postgres: bulk capability detected, performing bulk transfer...")
		}

		err = w.bulkTransfer(src, dstName, rows)
	} else {
		if PG_W_VERBOSE {
			log.Print("postgres: no bulk capability detected, performing normal transfer...")
		}

		err = w.normalTransfer(src, dstName, rows)
	}
	if err != nil {
		return err
	}

	return rows.Err()
}
func (w *genericPostgresWriter) ClearTable(tables []string) {
	if err := w.e.Begin("clear table"); err != nil {
		fmt.Println(err.Error())
	}
	for _, table := range tables {
		w.e.Submit(fmt.Sprintf("drop table %s;", table))
	}
	if err := w.e.Commit(); err != nil {
		fmt.Println(err.Error())
	}
}

/* how to do an UPSERT/MERGE in PostgreSQL
 * http://stackoverflow.com/questions/17267417/how-do-i-do-an-upsert-merge-insert-on-duplicate-update-in-postgresq */
func (w *genericPostgresWriter) MergeTable(src *common.Table, dstName, extraDstCond string, r common.Reader) error {
	mergeTableI := fmt.Sprintf("merge table %v into table %v",
		src.Name, dstName)
	if err := w.e.Begin(mergeTableI); err != nil {
		return err
	}

	/* create temporary table */
	tempTableQ := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (\n\t%v\n);\n", dstName, ColumnsSql(src))
	if err := w.e.Submit(tempTableQ); err != nil {
		return err
	}

	if PG_W_VERBOSE {
		log.Println("postgres: preparing to read values from source db")
	}

	if err := w.transferTable(src, dstName, r); err != nil {
		return err
	}

	if PG_W_VERBOSE {
		log.Print("postgres: rowscan done, creating merge statements")
	}
	return w.e.Commit()
}

func (w *genericPostgresWriter) Close() error {
	return w.e.Close()
}

type PostgresWriter struct {
	genericPostgresWriter
	db *sql.DB
}

//GetDB GetDB
func (w *PostgresWriter) GetDB() *sql.DB {
	return w.db
}

//NewPostgresWriter NewPostgresWriter
func NewPostgresWriter(conf *common.Config) (*PostgresWriter, error) {
	db, err := openDB(conf)
	if err != nil {
		return nil, err
	}

	executor, err := NewPgDbExecutor(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	errors := executor.Multiple("initializing DB connection (WARNING: connection pooling might mess with this)", postgresInit)
	if len(errors) > 0 {
		executor.Close()
		for _, err := range errors {
			log.Println("postgres error:", err)
		}
		return nil, errors[0]
	}

	return &PostgresWriter{db: db, genericPostgresWriter: genericPostgresWriter{executor, 64}}, nil
}

type PostgresFileWriter struct {
	genericPostgresWriter
}

func (p *PostgresFileWriter) GetDB() *sql.DB {
	return nil
}
func NewPostgresFileWriter(filename string) (*PostgresFileWriter, error) {
	executor, err := common.NewFileExecutor(filename)
	if err != nil {
		return nil, err
	}

	errors := executor.Multiple("initializing DB connection", postgresInit)
	if len(errors) > 0 {
		executor.Close()
		for _, err := range errors {
			log.Println("postgres error:", err)
		}
		return nil, errors[0]
	}

	return &PostgresFileWriter{genericPostgresWriter{executor, 256}}, err
}

//ColumnsSql ColumnsSql
func ColumnsSql(table *common.Table) string {
	colSQL := make([]string, 0, len(table.Columns))

	for _, col := range table.Columns {
		colSQL = append(colSQL, fmt.Sprintf("%v %v", col.Name, GenericToPostgresType(col.Type)))
	}

	pkCols := make([]string, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}

	/* add the primary key */
	colSQL = append(colSQL, fmt.Sprintf("PRIMARY KEY (%v)",
		strings.Join(pkCols, ", ")))

	return strings.Join(colSQL, ",\n\t")
}
