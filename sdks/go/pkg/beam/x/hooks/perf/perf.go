// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package perf is to add performance measuring hooks to a runner, such as cpu, or trace profiles.
package perf

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"runtime/pprof"
	"runtime/trace"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

// ProfCaptureHook is used by the harness to have the runner
// persist a trace record with the supplied name and comment.
// The type of trace can be determined by the prefix of the string.
//
// * prof: A profile compatible with traces produced by runtime/pprof
// * trace: A trace compatible with traces produced by runtime/trace
type ProfCaptureHook func(string, io.Reader) error

var profCaptureHookRegistry = make(map[string]ProfCaptureHook)
var enabledProfCaptureHooks []string

// RegisterProfCaptureHook registers a ProfCaptureHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterProfCaptureHook(name string, c ProfCaptureHook) {
	if _, exists := profCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterProfCaptureHook: %s registered twice", name))
	}
	profCaptureHookRegistry[name] = c

	if len(profCaptureHookRegistry) == 1 {
		var cpuProfBuf bytes.Buffer

		hf := func(opts []string) harness.Hook {
			enabledProfCaptureHooks = opts
			enabled := len(enabledProfCaptureHooks) > 0
			return harness.Hook{
				Req: func(_ context.Context, _ *fnpb.InstructionRequest) error {
					if !enabled {
						return nil
					}
					cpuProfBuf.Reset()
					return pprof.StartCPUProfile(&cpuProfBuf)
				},
				Resp: func(_ context.Context, req *fnpb.InstructionRequest, _ *fnpb.InstructionResponse) error {
					if !enabled {
						return nil
					}
					pprof.StopCPUProfile()
					for _, h := range enabledProfCaptureHooks {
						if err := profCaptureHookRegistry[h](fmt.Sprintf("cpu_prof%s", req.InstructionId), &cpuProfBuf); err != nil {
							return err
						}
					}
					return nil
				},
			}
		}
		harness.RegisterHook("prof", hf)
	}
}

// EnableProfCaptureHook actives a registered profile capture hook for a given pipeline.
func EnableProfCaptureHook(name string) {
	_, exists := profCaptureHookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("EnableProfCaptureHook: %s not registered", name))
	}

	for _, h := range enabledProfCaptureHooks {
		if h == name {
			// Registering the same hook twice isn't ideal, but allowable.
			return
		}
	}
	enabledProfCaptureHooks = append(enabledProfCaptureHooks, name)
	harness.EnableHook("prof", enabledProfCaptureHooks...)
}

// TraceCaptureHook ...
type TraceCaptureHook func(string, io.Reader) error

var traceCaptureHookRegistry = make(map[string]TraceCaptureHook)
var enabledTraceCaptureHooks []string

// RegisterTraceCaptureHook registers a TraceCaptureHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterTraceCaptureHook(name string, c TraceCaptureHook) {
	if _, exists := traceCaptureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterTraceCaptureHook: %s registered twice", name))
	}
	traceCaptureHookRegistry[name] = c
	if len(traceCaptureHookRegistry) == 1 {
		var traceProfBuf bytes.Buffer
		hf := func(opts []string) harness.Hook {
			enabledTraceCaptureHooks = opts
			enabled := len(enabledTraceCaptureHooks) > 0
			return harness.Hook{
				Req: func(_ context.Context, _ *fnpb.InstructionRequest) error {
					if !enabled {
						return nil
					}
					traceProfBuf.Reset()
					return trace.Start(&traceProfBuf)
				},
				Resp: func(_ context.Context, req *fnpb.InstructionRequest, _ *fnpb.InstructionResponse) error {
					if !enabled {
						return nil
					}
					trace.Stop()
					for _, h := range enabledTraceCaptureHooks {
						if err := traceCaptureHookRegistry[h](fmt.Sprintf("trace_prof%s", req.InstructionId), &traceProfBuf); err != nil {
							return err
						}
					}
					return nil
				},
			}
		}
		harness.RegisterHook("trace", hf)
	}
}

// EnableTraceCaptureHook actives a registered profile capture hook for a given pipeline.
func EnableTraceCaptureHook(name string) {
	if _, exists := traceCaptureHookRegistry[name]; !exists {
		panic(fmt.Sprintf("EnableTraceCaptureHook: %s not registered", name))
	}

	for _, h := range enabledTraceCaptureHooks {
		if h == name {
			// Registering the same hook twice isn't ideal, but allowable.
			return
		}
	}
	enabledTraceCaptureHooks = append(enabledTraceCaptureHooks, name)
	harness.EnableHook("trace", enabledTraceCaptureHooks...)
}
