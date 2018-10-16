package databaseio

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

//represents a record mapper
type rowMapper func(value reflect.Value) ([]interface{}, error)

//newQueryMapper creates a new record mapped
func newQueryMapper(columns []string, columnTypes []*sql.ColumnType, recordType reflect.Type) (rowMapper, error) {
	var val = reflect.New(recordType).Interface()
	if _, isLoader := val.(MapLoader); isLoader {
		return newQueryLoaderMapper(columns, columnTypes)
	} else if recordType.Kind() == reflect.Struct {
		return newQueryStructMapper(columns, recordType)
	}
	return nil, fmt.Errorf("unsupported type %s", recordType)
}

//newQueryStructMapper creates a new record mapper for supplied struct type
func newQueryStructMapper(columns []string, recordType reflect.Type) (rowMapper, error) {
	mappedFieldIndex, err := mapFields(columns, recordType)
	if err != nil {
		return nil, err
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

//newQueryStructMapper creates a new record mapper for supplied struct type
func newQueryLoaderMapper(columns []string, columnTypes []*sql.ColumnType) (rowMapper, error) {
	var record = make([]interface{}, len(columns))

	var valueProviders = make([]func(index int, values []interface{}), len(columns))
	var defaultProvider = func(index int, values []interface{}) {
		val := new(interface{})
		values[index] = &val
	}
	for i := range columns {
		valueProviders[i] = defaultProvider
		if len(columnTypes) == 0 {
			continue
		}
		dbTypeName := strings.ToLower(columnTypes[i].DatabaseTypeName())
		if strings.Contains(dbTypeName, "char") || strings.Contains(dbTypeName, "string") || strings.Contains(dbTypeName, "text") {
			valueProviders[i] = func(index int, values []interface{}) {
				val := ""
				values[index] = &val

			}
		} else if strings.Contains(dbTypeName, "int") {
			valueProviders[i] = func(index int, values []interface{}) {
				val := 0
				values[index] = &val
			}
		} else if strings.Contains(dbTypeName, "decimal") || strings.Contains(dbTypeName, "numeric") || strings.Contains(dbTypeName, "float") {
			valueProviders[i] = func(index int, values []interface{}) {
				val := 0.0
				values[index] = &val

			}
		} else if strings.Contains(dbTypeName, "time") || strings.Contains(dbTypeName, "date") {
			valueProviders[i] = func(index int, values []interface{}) {
				val := time.Now()
				values[index] = &val

			}
		} else if strings.Contains(dbTypeName, "bool") {
			valueProviders[i] = func(index int, values []interface{}) {
				val := false
				values[index] = &val
			}
		} else {
			valueProviders[i] = func(index int, values []interface{}) {
				val := reflect.New(columnTypes[i].ScanType()).Elem().Interface()
				values[index] = &val
			}
		}
	}
	var mapper = func(value reflect.Value) ([]interface{}, error) {
		for i := range columns {
			valueProviders[i](i, record)
		}
		return record, nil
	}
	return mapper, nil
}

//newQueryMapper creates a new record mapped
func newWriterRowMapper(columns []string, recordType reflect.Type) (rowMapper, error) {
	mappedFieldIndex, err := mapFields(columns, recordType)
	if err != nil {
		return nil, err
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
