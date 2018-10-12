package databaseio

import (
	"testing"
	"time"
	"reflect"
	"github.com/stretchr/testify/assert"
)

func Test_queryRecordMapperProvider(t *testing.T) {

	type User struct {
		Id int
		local bool
		DateOfBirth time.Time
		NameTest string `column:"name"`
		Random float64
	}

	mapper, err := newQueryRecordMapper([]string{"id", "name", "random", "date_of_birth"}, nil, reflect.TypeOf(User{}))
	if ! assert.Nil(t, err) {
		return
	}

	aUser := &User{}
	record, err := mapper(reflect.ValueOf(aUser))
	if ! assert.Nil(t, err) {
		return
	}
	id, ok := record[0].(*int)
	assert.True(t, ok)
	*id = 10

	name, ok := record[1].(*string)
	assert.True(t, ok)
	*name = "test"

	random, ok := record[2].(*float64)
	assert.True(t, ok)
	*random = 1.2

	dob, ok := record[3].(*time.Time)
	assert.True(t, ok)
	var now = time.Now()
	*dob = now


	assert.EqualValues(t, &User{
		Id:*id,
		DateOfBirth:*dob,
		NameTest:*name,
		Random:*random,
	}, aUser)
}



func Test_writerRecordMapperProvider(t *testing.T) {
	type User struct {
		Id int
		local bool
		DateOfBirth time.Time
		NameTest string `column:"name"`
		Random float64
	}

	mapper, err := newWriterRecordMapper([]string{"id", "name", "random", "date_of_birth"}, reflect.TypeOf(User{}))
	if ! assert.Nil(t, err) {
		return
	}
	aUser := &User{
		Id:2,
		NameTest:"abc",
		Random:1.6,
		DateOfBirth:time.Now(),
	}

	record, err := mapper(reflect.ValueOf(aUser))
	if ! assert.Nil(t, err) {
		return
	}
	assert.EqualValues(t, 2, record[0])
	assert.EqualValues(t, "abc", record[1])
	assert.EqualValues(t, 1.6, record[2])
	assert.EqualValues(t, aUser.DateOfBirth, record[3])

}

