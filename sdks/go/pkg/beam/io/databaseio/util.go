package databaseio

import (
	"fmt"
	"reflect"
	"strings"
)

//mapFields maps column into field index in record type
func mapFields(columns []string, recordType reflect.Type) ([]int, error) {
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
	return mappedFieldIndex, nil
}

func asDereferenceSlice(aSlice []interface{}) {
	for i, value := range aSlice {
		if value == nil {
			continue
		}
		aSlice[i] = reflect.ValueOf(value).Elem().Interface()

	}
}

func asMap(keys []string, values []interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for i, key := range keys {
		result[key] = values[i]
	}
	return result
}
