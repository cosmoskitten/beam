package databaseio

import (
	"database/sql"
	"fmt"
	"golang.org/x/net/context"
	"strings"
)

// A Writer returns a row of data to be inserted into a table.
type Writer interface {
	SaveData() (map[string]interface{}, error)
}

type writer struct {
	batchSize    int
	table        string
	sqlTemplate  string
	valueTempate string
	binding      []interface{}
	columnCount  int
	rowCount     int
	totalCount   int
}

func (w *writer) add(row []interface{}) error {
	w.rowCount++
	w.totalCount++
	if len(row) != w.columnCount {
		return fmt.Errorf("expected %v row values, but had: %v", w.columnCount, len(row))
	}
	w.binding = append(w.binding, row...)
	return nil
}

func (w *writer) write(ctx context.Context, db *sql.DB) error {
	values := strings.Repeat(w.valueTempate+",", w.rowCount)
	SQL := w.sqlTemplate + string(values[:len(values)-1])
	resultSet, err := db.ExecContext(ctx, SQL, w.binding...)
	if err != nil {
		return err
	}
	affected, _ := resultSet.RowsAffected()
	if int(affected) != w.rowCount {
		return fmt.Errorf("expected to write: %v, but written: %v", w.rowCount, affected)
	}
	w.binding = []interface{}{}
	w.rowCount = 0
	return nil
}

func (w *writer) writeBatchIfNeeded(ctx context.Context, db *sql.DB) error {
	if w.rowCount >= w.batchSize {
		return w.write(ctx, db)
	}
	return nil
}

func (w *writer) writeIfNeeded(ctx context.Context, db *sql.DB) error {
	if w.rowCount >= 0 {
		return w.write(ctx, db)
	}
	return nil
}

func newWriter(batchSize int, table string, columns []string) (*writer, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	values := strings.Repeat("?,", len(columns))
	return &writer{
		batchSize:    batchSize,
		columnCount:  len(columns),
		table:        table,
		binding:      make([]interface{}, 0),
		sqlTemplate:  fmt.Sprintf("INSERT INTO %v(%v) VALUES", table, strings.Join(columns, ",")),
		valueTempate: fmt.Sprintf("(%s)", values[:len(values)-1]),
	}, nil
}
