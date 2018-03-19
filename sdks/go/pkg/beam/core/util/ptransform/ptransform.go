package ptransform

import "context"

type key string

const ptransformKey key = "beam:ptransform"

// SetID sets the id of the current PTransform.
func SetID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ptransformKey, id)
}

// TryGetID extracts the id of the PTransform, if it is set.
func TryGetID(ctx context.Context) (string, bool) {
	id := ctx.Value(ptransformKey)
	if id == nil {
		return "", false
	}
	return id.(string), true
}
