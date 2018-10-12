package databaseio

import (
	"strings"
	"fmt"
	"database/sql"
	"golang.org/x/net/context"
)

type writer struct {
	batchSize int
	table string
	sqlTemplate string
	valueTempate string
	binding []interface{}
	columnCount int
	recordCount int
	totalCount int
}

func (w *writer) add(record []interface{}) error {
	w.recordCount++
	w.totalCount++
	if len(record) != w.columnCount {
		return fmt.Errorf("expected %v record values, but had: %v", w.columnCount, len(record))
	}
	w.binding = append(w.binding, record...)
	return nil
}


func (w *writer) write(db *sql.DB, ctx context.Context) error {
	values := strings.Repeat(w.valueTempate +",", w.recordCount)
	SQL := w.sqlTemplate + string(values[:len(values)-1])
	resultSet, err := db.ExecContext(ctx, SQL, w.binding...)
	if err != nil {
		return err
	}
	affected, _ := resultSet.RowsAffected()
	if int(affected) != w.recordCount {
		return fmt.Errorf("expected to write: %v, but written: %v", w.recordCount, affected)
	}
	w.binding = []interface{}{}
	w.recordCount = 0
	return nil
}


func (w *writer) writeBatchIfNeeded(db *sql.DB, ctx context.Context) error {
	if w.recordCount >= w.batchSize {
		return w.write(db, ctx)
	}
	return nil
}


func (w *writer) writeIfNeeded(db *sql.DB, ctx context.Context) error {
	if w.recordCount >= 0 {
		return w.write(db, ctx)
	}
	return nil
}


func newWriter(batchSize int, table string, columns []string) (*writer, error){
	if len(columns) == 0 {
		return nil, fmt.Errorf("columns were empty")
	}
	values := strings.Repeat("?,", len(columns))
	return &writer{
		batchSize:batchSize,
		columnCount: len(columns),
		table: table,
		binding: make([]interface{}, 0),
		sqlTemplate:fmt.Sprintf("INSERT INTO %v(%v) VALUES", table, strings.Join(columns, ",")),
		valueTempate:fmt.Sprintf("(%s)", values[:len(values)-1]),
	}, nil
}
