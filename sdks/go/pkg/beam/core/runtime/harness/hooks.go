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
	"io"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"

	"google.golang.org/grpc"
)

const hooksNamespaceKey = "__hooks:"
const sessionCaptureHookKey = hooksNamespaceKey + "session_capture"
const traceCaptureHookKey = hooksNamespaceKey + "trace_capture"
const grpcHookKey = hooksNamespaceKey + "grpc"

func bindHooks() {
	if sessionCapture := runtime.GlobalOptions.Get(sessionCaptureHookKey); sessionCapture != "" {
		capture = sessionCaptureHook(sessionCapture)
	}

	if traceCapture := runtime.GlobalOptions.Get(traceCaptureHookKey); traceCapture != "" {
		profileWriter = traceCaptureHook(traceCapture)
	}

	if grpc := runtime.GlobalOptions.Get(grpcHookKey); grpc != "" {
		grpcHooks := grpcHook(grpc)
		if grpcHooks.Dialer != nil {
			grpcx.Dial = grpcHooks.Dialer
		}
	}
}

// SessionCaptureHook writes the messaging content consumed and
// produced by the worker, allowing the data to be used as
// an input for the session runner. Since workers can exist
// in a variety of environments, this allows the runner
// to tailor the behavior best for its particular needs.
type SessionCaptureHook io.WriteCloser

var sessionCaptureHookRegistry = make(map[string]SessionCaptureHook)

// RegisterSessionCaptureHook registers a SessionCaptureHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterSessionCaptureHook(name string, c SessionCaptureHook) {
	if _, exists := sessionCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterSessionCaptureHook: %s registered twice", name))
	}
	sessionCaptureHookRegistry[name] = c
}

// RequestSessionCaptureHook is called to request the use of a hook in a pipeline.
// It updates the supplied pipelines to capture this request.
func RequestSessionCaptureHook(name string, o *runtime.RawOptions) {
	o.Options[sessionCaptureHookKey] = name
}

func sessionCaptureHook(name string) SessionCaptureHook {
	c, exists := sessionCaptureHookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("sessionCaptureHook: %s not registered", name))
	}
	return c
}

// TraceCaptureHook is used by the harness to have the runner
// persist a trace record with the supplied name and comment.
// The type of trace can be determined by the prfix of the string
// cpu: A profile compatible with traces produced by runtime/pprof
// trace: A trace compatible with traces produced by runtime/trace
// TODO(wcn): define and document future formats here.
type TraceCaptureHook func(string, io.Reader) error

var traceCaptureHookRegistry = make(map[string]TraceCaptureHook)

// RegisterTraceCaptureHook registers a TraceCaptureHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterTraceCaptureHook(name string, c TraceCaptureHook) {
	if _, exists := traceCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterTraceCaptureHook: %s registered twice", name))
	}
	traceCaptureHookRegistry[name] = c
}

func traceCaptureHook(name string) TraceCaptureHook {
	c, exists := traceCaptureHookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("traceCaptureHook: %s not registered", name))
	}
	return c
}

// RequestTraceCaptureHook is called to request the use of the trace capture
// hook in a pipeline.
func RequestTraceCaptureHook(name string, o *runtime.RawOptions) {
	o.Options[traceCaptureHookKey] = name
}

// GrpcHook allow a runner to tailor various aspects of gRPC
// communication with the FnAPI harness. Each member of the struct
// is optional; the default behavior will be used if a value is not
// supplied.
type GrpcHook struct {
	// Dialer allows the runner to customize the gRPC dialing behavior.
	Dialer func(context.Context, string, time.Duration) (*grpc.ClientConn, error)
	// TODO(wcn): expose other hooks here.
}

var grpcHookRegistry = make(map[string]GrpcHook)

// RegisterGrpcHook registers a GrpcHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterGrpcHook(name string, c GrpcHook) {
	if _, exists := grpcHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterGrpcHook: %s registered twice", name))
	}
	grpcHookRegistry[name] = c
}

func grpcHook(name string) GrpcHook {
	c, exists := grpcHookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("grpcHook: %s not registered", name))
	}
	return c
}

// RequestGrpcHook is called to request the use of the gRPC
// hook in a pipeline.
func RequestGrpcHook(name string, o *runtime.RawOptions) {
	o.Options[grpcHookKey] = name
}
