package perf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/pprof"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

var (
	sessionCaptureHookKey = harness.HookKey("session_capture")
	traceCaptureHookKey   = harness.HookKey("trace_capture")
)

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

	if len(traceCaptureHookRegistry) == 1 {
		var cpuProfBuf bytes.Buffer
		harness.RegisterRequestHook("trace", func(_ context.Context, _ *fnpb.InstructionRequest) error {
			cpuProfBuf.Reset()
			return pprof.StartCPUProfile(&cpuProfBuf)
		})
		harness.RegisterResponseHook("trace", func(_ context.Context, req *fnpb.InstructionRequest, _ *fnpb.InstructionResponse) error {
			pprof.StopCPUProfile()
			for _, h := range traceCaptureHookRegistry {
				if err := h(fmt.Sprintf("cpu_prof%s", req.InstructionId), &cpuProfBuf); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

// RequestTraceCaptureHook is called to request the use of the trace capture
// hook in a pipeline.
func RequestTraceCaptureHook(name string, o *runtime.RawOptions) {
	o.Options[traceCaptureHookKey] = name
}

// SessionCaptureHook writes the messaging content consumed and
// produced by the worker, allowing the data to be used as
// an input for the session runner. Since workers can exist
// in a variety of environments, this allows the runner
// to tailor the behavior best for its particular needs.
type SessionCaptureHook io.WriteCloser

var sessionCaptureHookRegistry = make(map[string]SessionCaptureHook)

// RegisterSessionCaptureHook registers a SessionCaptureHook for the
// supplied identifier. Only one session capture hook can be registered.
func RegisterSessionCaptureHook(name string, c SessionCaptureHook) {
	if len(sessionCaptureHookRegistry) == 1 {
		panic("RegisterSessionCaptureHook already registered")
	}
	sessionCaptureHookRegistry[name] = c

	harness.RegisterInitHook("session", func(_ context.Context) error {
		harness.Capture = c
		return nil
	})
}

// RequestSessionCaptureHook is called to request the use of a hook in a pipeline.
// It updates the supplied pipelines to capture this request.
func RequestSessionCaptureHook(name string, o *runtime.RawOptions) {
	o.Options[sessionCaptureHookKey] = name
}
