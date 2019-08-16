package reflectx

import (
	"reflect"
	"testing"
)

func testFunction() int {
	return 42
}

func TestXxx(t *testing.T) {
	val := reflect.ValueOf(testFunction)
	fi := uintptr(val.Pointer())
	typ := val.Type()

	callable := LoadFunction(fi, typ)

	cv := reflect.ValueOf(callable)
	out := cv.Call(nil)
	if len(out) != 1 {
		t.Errorf("got %d return values, wanted 1.", len(out))
	}
	// TODO: check type?
	if out[0].Int() != 42 {
		t.Errorf("got %d, wanted 42", out[0].Int())
	}
}
