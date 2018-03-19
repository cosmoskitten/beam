package ptransform

import (
	"context"
	"testing"
)

func TestSetAndTryGetID(t *testing.T) {
	t.Run("ordinary case - id is set", func(t *testing.T) {
		ctx := context.Background()
		want := "my awesome ptransform id"
		ctx = SetID(ctx, want)
		if got, ok := TryGetID(ctx); !ok {
			t.Fatalf("TryGetID(%v) = %q, not ok; want %q, ok", ctx, got, want)
		} else if got != want {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		}
	})
	t.Run("id unset", func(t *testing.T) {
		ctx := context.Background()
		want := ""
		if got, ok := TryGetID(ctx); ok {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		} else if got != want {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		}
	})
}
