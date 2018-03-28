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

// Package session supplies a hook for recording data that can be consumed by the
// session runner to replay workflow execution.
package session

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/harness"
)

// CaptureHook writes the messaging content consumed and
// produced by the worker, allowing the data to be used as
// an input for the session runner. Since workers can exist
// in a variety of environments, this allows the runner
// to tailor the behavior best for its particular needs.
type CaptureHook io.WriteCloser

var captureHookRegistry = make(map[string]CaptureHook)
var enabledCaptureHook string

// RegisterHook registers a CaptureHook for the
// supplied identifier.
func RegisterHook(name string, c CaptureHook) {
	if _, exists := captureHookRegistry[name]; exists {
		panic(fmt.Sprintf("RegisterSessionCaptureHook: %s registered twice", name))
	}
	captureHookRegistry[name] = c
	if len(captureHookRegistry) == 1 {
		hf := func(opts []string) harness.Hook {
			return harness.Hook{
				Init: func(_ context.Context) error {
					if len(opts) > 0 {
						harness.Capture = captureHookRegistry[opts[0]]
					}
					return nil
				},
			}
		}

		harness.RegisterHook("session", hf)
	}
}

// EnableHook is called to request the use of a hook in a pipeline.
// It updates the supplied pipelines to capture this request.
func EnableHook(name string) {
	if _, exists := captureHookRegistry[name]; !exists {
		panic(fmt.Sprintf("EnableHook: %s not registered", name))
	}
	if enabledCaptureHook != "" && enabledCaptureHook != name {
		panic(fmt.Sprintf("EnableHook: can't enable hook %s, hook %s already enabled", name, enabledCaptureHook))
	}

	enabledCaptureHook = name
	harness.EnableHook("session", enabledCaptureHook)
}
