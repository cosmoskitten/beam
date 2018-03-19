package ptransform

import "context"

var key = struct{ Key string }{Key: "beam:ptransform"}

// SetID sets the id of the current PTransform.
func SetID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key, id)
}

// TryGetID extracts the id of the PTransform, if it is set.
func TryGetID(ctx context.Context) (string, bool) {
	id := ctx.Value(key)
	if id == nil {
		return "", false
	}
	return id.(string), true
}
