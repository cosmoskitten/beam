package coderx

import (
	"testing"
)

func TestString(t *testing.T) {
	tests := []interface{}{
		"",
		"A novel set of characters",
		"Hello, 世界",
	}

	for _, v := range tests {
		data := encString(v)
		result := decString(data)

		if v != result {
			t.Errorf("dec(enc(%v)) = %v, want %v", v, result, v)
		}
	}
}
