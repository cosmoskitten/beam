package hooks

import (
	"context"
	"fmt"
	"testing"

	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

type contextKey string

func initializeHooks() {
	activeHooks["test"] = Hook{
		Init: func(ctx context.Context) (context.Context, error) {
			return context.WithValue(ctx, contextKey("init_key"), "value"), nil
		},
		Req: func(ctx context.Context, req *fnpb.InstructionRequest) (context.Context, error) {
			return context.WithValue(ctx, contextKey("req_key"), "value"), nil
		},
	}
}

func TestInitContextPropagation(t *testing.T) {
	initializeHooks()
	ctx := context.Background()
	var err error

	expected := `context.Background.WithValue("init_key", "value")`
	ctx, err = RunInitHooks(ctx)
	if err != nil {
		t.Errorf("got %v error, wanted no error", err)
	}
	actual := ctx.(fmt.Stringer).String()
	if actual != expected {
		t.Errorf("Got %s, wanted %s", actual, expected)
	}
}

func TestRequestContextPropagation(t *testing.T) {
	initializeHooks()
	ctx := context.Background()

	expected := `context.Background.WithValue("req_key", "value")`
	ctx = RunRequestHooks(ctx, nil)
	actual := ctx.(fmt.Stringer).String()
	if actual != expected {
		t.Errorf("Got %s, wanted %s", actual, expected)
	}
}
