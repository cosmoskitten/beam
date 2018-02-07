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
	"encoding/json"
	"fmt"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

var enabledHooks = make(map[string][]string)

// A Hook is a collection of registered functions that...
// TODO(wcn): steal content from client.HttpTrace
type Hook struct {
	Init        InitHook
	Req         RequestHook
	Resp        ResponseHook
	Serialize   func() []string
	Deserialize func(...string)
}

// InitHook is a hook that is called when the harness
// initializes.
type InitHook func(context.Context) error

var hookRegistry = make(map[string]Hook)

// RegisterHook registers a InitHook for the
// supplied identifier.
func RegisterHook(name string, h Hook) {
	if h.Serialize == nil {
		panic(fmt.Sprintf("Hook %s is missing Serialize method", name))
	}
	if h.Deserialize == nil {
		panic(fmt.Sprintf("Hook %s is missing Deserialize method", name))
	}

	hookRegistry[name] = h
}

func runInitHooks(ctx context.Context) error {
	// If an init hook fails to complete, the invariants of the
	// system are compromised and we can't run a workflow.
	// The hooks can run in any order. They should not be
	// interdependent or interfere with each other.
	for _, h := range hookRegistry {
		if h.Init != nil {
			if err := h.Init(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// RequestHook is called when handling a Fn API instruction.
type RequestHook func(context.Context, *fnpb.InstructionRequest) error

func runRequestHooks(ctx context.Context, req *fnpb.InstructionRequest) {
	// The request hooks should not modify the request.
	// TODO(wcn): pass the request by value to enforce? That's a perf hit.
	// I'd rather trust users to do the right thing.
	for n, h := range hookRegistry {
		if h.Req != nil {
			if err := h.Req(ctx, req); err != nil {
				log.Infof(ctx, "request hook %s failed: %v", n, err)
			}
		}
	}
}

// ResponseHook is called when sending a Fn API instruction response.
type ResponseHook func(context.Context, *fnpb.InstructionRequest, *fnpb.InstructionResponse) error

func runResponseHooks(ctx context.Context, req *fnpb.InstructionRequest, resp *fnpb.InstructionResponse) {
	for n, h := range hookRegistry {
		if h.Resp != nil {
			if err := h.Resp(ctx, req, resp); err != nil {
				log.Infof(ctx, "response hook %s failed: %v", n, err)
			}
		}
	}
}

// SerializeHooks serializes the activated hooks and their configuration into a JSON string
// that can be deserialized later by the runner.
func SerializeHooks() string {
	data, err := json.Marshal(enabledHooks)
	if err != nil {
		// Shouldn't happen, since all the data is strings.
		panic(fmt.Sprintf("Couldn't serialize hooks: %v", err))
	}
	return string(data)
}

// deserializeHooks extracts the hook configuration information from the options and calls the hook deserialize
// method with the supplied options.
func deserializeHooks() {
	cfg := runtime.GlobalOptions.Get("hooks")
	if err := json.Unmarshal([]byte(cfg), enabledHooks); err != nil {
		// Shouldn't happen
		panic(fmt.Sprintf("DeserializeHooks failed: %v", err))
	}

	for h, opts := range enabledHooks {
		hookRegistry[h].Deserialize(opts...)
	}
}
