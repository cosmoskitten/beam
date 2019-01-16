package primitives

import (
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

// TestMain invokes ptest.Main to allow running these tests on
// non-direct runners.
func TestMain(m *testing.M) {
	ptest.Main(m)
}
