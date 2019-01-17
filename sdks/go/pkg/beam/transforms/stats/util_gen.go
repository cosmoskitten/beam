// File generated by specialize. Do not edit.

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

package stats

// This file needs to be *after* all the other generated files in the stats package
// so that their generated functions exist when this one is run.
// `go generate` executes go:generate commands in lexical file order, top to bottom.

//go:generate go install github.com/apache/beam/sdks/go/cmd/starcgen
//go:generate starcgen --package=stats --identifiers=mapFn,meanFn,maxIntFn,minIntFn,sumIntFn,maxInt8Fn,minInt8Fn,sumInt8Fn,maxInt16Fn,minInt16Fn,sumInt16Fn,maxInt32Fn,minInt32Fn,sumInt32Fn,maxInt64Fn,minInt64Fn,sumInt64Fn,maxUintFn,minUintFn,sumUintFn,maxUint8Fn,minUint8Fn,sumUint8Fn,maxUint16Fn,minUint16Fn,sumUint16Fn,maxUint32Fn,minUint32Fn,sumUint32Fn,maxUint64Fn,minUint64Fn,sumUint64Fn,maxFloat32Fn,minFloat32Fn,sumFloat32Fn,maxFloat64Fn,minFloat64Fn,sumFloat64Fn
