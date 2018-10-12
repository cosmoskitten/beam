package databaseio

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

//represents a record mapper
type recordMapper func(value reflect.Value) ([]interface{}, error)

//newQueryRecordMapper creates a new record mapped
func newQueryRecordMapper(columns []string, columnTypes []*sql.ColumnType, recordType reflect.Type) (recordMapper, error) {
	//TODO columnType to be used with a map provider record Type
	var indexedFields = map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		fieldName := recordType.Field(i).Name
		indexedFields[fieldName] = i
		indexedFields[strings.ToLower(fieldName)] = i //to account for various matching strategies
		aTag := recordType.Field(i).Tag
		if column := aTag.Get("column"); column != "" {
			indexedFields[column] = i
		}
	}

	var mappedFieldIndex = make([]int, recordType.NumField())
	for i, column := range columns {
		fieldIndex, ok := indexedFields[column]
		if !ok {
			fieldIndex, ok = indexedFields[strings.ToLower(column)]
		}
		if !ok {
			fieldIndex, ok = indexedFields[strings.Replace(strings.ToLower(column), "_", "", strings.Count(column, "_"))]
		}
		if !ok {
			return nil, fmt.Errorf("failed to matched a %v field for SQL column: %v", recordType, column)
		}
		mappedFieldIndex[i] = fieldIndex
	}
	var record = make([]interface{}, recordType.NumField())
	var mapper = func(value reflect.Value) ([]interface{}, error) {
		value = value.Elem() //T = *T
		for i, fieldIndex := range mappedFieldIndex {
			record[i] = value.Field(fieldIndex).Addr().Interface()
		}
		return record, nil
	}
	return mapper, nil
}

//newQueryRecordMapper creates a new record mapped
func newWriterRecordMapper(columns []string, recordType reflect.Type) (recordMapper, error) {
	//TODO columnType to be used with a map provider record Type
	var indexedFields = map[string]int{}
	for i := 0; i < recordType.NumField(); i++ {
		fieldName := recordType.Field(i).Name
		indexedFields[fieldName] = i
		indexedFields[strings.ToLower(fieldName)] = i //to account for various matching strategies
		aTag := recordType.Field(i).Tag
		if column := aTag.Get("column"); column != "" {
			indexedFields[column] = i
		}
	}

	var mappedFieldIndex = make([]int, len(columns))
	for i, column := range columns {
		fieldIndex, ok := indexedFields[column]
		if !ok {
			fieldIndex, ok = indexedFields[strings.ToLower(column)]
		}
		if !ok {
			fieldIndex, ok = indexedFields[strings.Replace(strings.ToLower(column), "_", "", strings.Count(column, "_"))]
		}
		if !ok {
			return nil, fmt.Errorf("failed to matched a %v field for SQL column: %v", recordType, column)
		}
		mappedFieldIndex[i] = fieldIndex
	}
	columnCount := len(columns)
	var mapper = func(value reflect.Value) ([]interface{}, error) {
		var record = make([]interface{}, columnCount)
		if value.Kind() == reflect.Ptr {
			value = value.Elem() //T = *T
		}
		for i, fieldIndex := range mappedFieldIndex {
			record[i] = value.Field(fieldIndex).Interface()
		}
		return record, nil
	}
	return mapper, nil
}
