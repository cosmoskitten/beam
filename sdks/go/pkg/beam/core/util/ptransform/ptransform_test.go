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

package ptransform

import (
	"context"
	"testing"
)

func TestSetAndTryGetID(t *testing.T) {
	t.Run("ordinary case - id is set", func(t *testing.T) {
		ctx := context.Background()
		want := "my awesome ptransform id"
		ctx = SetID(ctx, want)
		if got, ok := TryGetID(ctx); !ok {
			t.Fatalf("TryGetID(%v) = %q, not ok; want %q, ok", ctx, got, want)
		} else if got != want {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		}
	})
	t.Run("id unset", func(t *testing.T) {
		ctx := context.Background()
		want := ""
		if got, ok := TryGetID(ctx); ok {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		} else if got != want {
			t.Fatalf("TryGetID(%v) = %q, ok; want %q, ok", ctx, got, want)
		}
	})
}
