// Hooks allow runners to tailor execution of the worker to allow for customization
// of features used by the harness.
//
// Examples of customization:
//
// gRPC integration
// session recording
// profile recording
//
// Registration methods for hooks must be called prior to calling beam.Init()
// Request methods for hooks must be called as part of building the pipeline
// request for the runner's Execute method.

package harness

import (
	"context"
	"fmt"

	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

const hooksNamespaceKey = "__hooks:"

// HookKey returns a hook registration key for the specified input.
func HookKey(in string) string {
	return hooksNamespaceKey + in
}

// InitHook is a hook that is called when the harness
// initializes.
type InitHook func(context.Context) error

var initHookRegistry = make(map[string]InitHook)

// RegisterInitHook registers a InitHook for the
// supplied identifier.
func RegisterInitHook(name string, c InitHook) {
	initHookRegistry[name] = c
}

func runInitHooks(ctx context.Context) error {
	for _, h := range initHookRegistry {
		if err := h(ctx); err != nil {
			return err
		}
	}
	return nil
}

// RequestHook is called when handling a Fn API instruction.
type RequestHook func(context.Context, *fnpb.InstructionRequest) error

var requestHookRegistry = make(map[string]RequestHook)

// RegisterRequestHook registers a RequestHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterRequestHook(name string, c RequestHook) {
	if _, exists := requestHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterRequestHook: %s registered twice", name))
	}
	requestHookRegistry[name] = c
}

func runRequestHooks(ctx context.Context, req *fnpb.InstructionRequest) error {
	for _, h := range requestHookRegistry {
		if err := h(ctx, req); err != nil {
			return err
		}
	}
	return nil
}

// ResponseHook is called when sending a Fn API instruction response.
type ResponseHook func(context.Context, *fnpb.InstructionRequest, *fnpb.InstructionResponse) error

var responseHookRegistry = make(map[string]ResponseHook)

// RegisterResponseHook registers a ResponseHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterResponseHook(name string, c ResponseHook) {
	if _, exists := responseHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterResponseHook: %s registered twice", name))
	}
	responseHookRegistry[name] = c
}

func runResponseHooks(ctx context.Context, req *fnpb.InstructionRequest, resp *fnpb.InstructionResponse) error {
	for _, h := range responseHookRegistry {
		if err := h(ctx, req, resp); err != nil {
			return err
		}
	}
	return nil
}
