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

package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
	"github.com/apache/beam/sdks/go/pkg/beam/util/grpcx"
	"google.golang.org/grpc"
)

var grpcHookKey = harness.HookKey("grpc")

// Hook allow a runner to tailor various aspects of gRPC
// communication with the FnAPI harness. Each member of the struct
// is optional; the default behavior will be used if a value is not
// supplied.
type Hook struct {
	// Dialer allows the runner to customize the gRPC dialing behavior.
	Dialer func(context.Context, string, time.Duration) (*grpc.ClientConn, error)
	// TODO(wcn): expose other hooks here.
}

var grpcHookRegistry = make(map[string]Hook)

// RegisterGrpcHook registers a GrpcHook for the
// supplied identifier. It panics if the same identifier is
// registered twice.
func RegisterGrpcHook(name string, c Hook) {
	if _, exists := grpcHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterGrpcHook: %s registered twice", name))
	}
	grpcHookRegistry[name] = c

	harness.RegisterInitHook("grpc", func(_ context.Context) error {
		if grpc := runtime.GlobalOptions.Get(grpcHookKey); grpc != "" {
			grpcHooks := grpcHook(grpc)
			if grpcHooks.Dialer != nil {
				// TODO(wcn): figure this out.
				grpcx.Dial = grpcHooks.Dialer
			}
		}
		return nil
	})
}

func grpcHook(name string) Hook {
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
