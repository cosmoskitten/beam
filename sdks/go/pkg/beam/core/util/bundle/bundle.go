package bundle

import "context"

type key string

const bundleKey key = "beam:bundle"

// SetID sets the id of the current Bundle.
func SetID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, bundleKey, id)
}

// TryGetID extracts the id of the Bundle, if it is set.
func TryGetID(ctx context.Context) (string, bool) {
	id := ctx.Value(bundleKey)
	if id == nil {
		return "", false
	}
	return id.(string), true
}
