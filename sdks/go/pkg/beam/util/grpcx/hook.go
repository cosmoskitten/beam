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

package grpcx

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"google.golang.org/grpc"
)

// Hook allow a runner to customize various aspects of gRPC
// communication with the FnAPI harness. Each member of the struct
// is optional; the default behavior will be used if a value is not
// supplied.
type Hook struct {
	// Dialer allows the runner to customize the gRPC dialing behavior.
	Dialer func(context.Context, string, time.Duration) (*grpc.ClientConn, error)
	// TODO(wcn): expose other hooks here.
}

var hookRegistry = make(map[string]Hook)
var enabledGrpcHook string

// RegisterHook registers a Hook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterHook(name string, c Hook) {
	if _, exists := hookRegistry[name]; exists {
		panic(fmt.Sprintf("grpc.Hook: %s registered twice", name))
	}
	hookRegistry[name] = c

	hf := func(opts []string) hooks.Hook {
		return hooks.Hook{
			Init: func(_ context.Context) error {
				if len(opts) == 0 {
					return nil
				}
				grpcHook := hookRegistry[opts[0]]
				if grpcHook.Dialer != nil {
					Dial = grpcHook.Dialer
				}
				return nil
			},
		}
	}
	hooks.RegisterHook("grpc", hf)
}

// EnableHook is called to request the use of the gRPC
// hook in a pipeline.
func EnableHook(name string) {
	_, exists := hookRegistry[name]
	if !exists {
		panic(fmt.Sprintf("EnableHook: %s not registered", name))
	}
	// Only one hook can be enabled. If the pipeline has two conflicting views about how to use gRPC
	// that won't end well.
	if enabledGrpcHook != "" && enabledGrpcHook != name {
		panic(fmt.Sprintf("EnableHook: can't enable hook %s, hook %s already enabled", name, enabledGrpcHook))
	}
	enabledGrpcHook = name
	hooks.EnableHook("grpc", enabledGrpcHook)
}
