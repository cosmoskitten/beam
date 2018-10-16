package databaseio

// MapLoader calls on LoadMap method with with a fetched row as map.
type MapLoader interface {
	LoadMap(row map[string]interface{}) error
}

// SliceLoader calls LoadSlice method with a fetched row as slice.
type SliceLoader interface {
	LoadSlice(row []interface{}) error
}
