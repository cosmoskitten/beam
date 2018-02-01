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

package coder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// ErrVarIntTooLong indicates a data corruption issue that needs special
// handling by callers of decode. TODO(herohde): have callers perform
// this special handling.
var ErrVarIntTooLong = errors.New("varint too long")

// NewVarIntZ returns a varint coder for the given integer type. It uses a zig-zag scheme,
// which is _different_ from the Beam standard coding scheme.
func NewVarIntZ(t reflect.Type) (*CustomCoder, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewCustomCoder("varintz", t, encVarIntZ, decVarIntZ)
	default:
		return nil, fmt.Errorf("not a signed integer type: %v", t)
	}
}

// NewVarUintZ returns a uvarint coder for the given integer type. It uses a zig-zag scheme,
// which is _different_ from the Beam standard coding scheme.
func NewVarUintZ(t reflect.Type) (*CustomCoder, error) {
	switch t.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return NewCustomCoder("varuintz", t, encVarUintZ, decVarUintZ)
	default:
		return nil, fmt.Errorf("not a unsigned integer type: %v", t)
	}
}

func encVarIntZ(v typex.T) []byte {
	var val int64
	switch n := v.(type) {
	case int:
		val = int64(n)
	case int8:
		val = int64(n)
	case int16:
		val = int64(n)
	case int32:
		val = int64(n)
	case int64:
		val = n
	default:
		panic(fmt.Sprintf("received unknown value type: want a signed integer:, got %T", n))
	}
	ret := make([]byte, binary.MaxVarintLen64)
	size := binary.PutVarint(ret, val)
	return ret[:size]
}

func decVarIntZ(t reflect.Type, data []byte) (typex.T, error) {
	n, size := binary.Varint(data)
	if size <= 0 {
		return nil, fmt.Errorf("invalid varintz encoding for: %v", data)
	}
	switch t.Kind() {
	case reflect.Int:
		return int(n), nil
	case reflect.Int8:
		return int8(n), nil
	case reflect.Int16:
		return int16(n), nil
	case reflect.Int32:
		return int32(n), nil
	case reflect.Int64:
		return n, nil
	}
	panic(fmt.Sprintf("unreachable statement: expected a signed integer, got %v", t))
}

func encVarUintZ(v typex.T) []byte {
	var val uint64
	switch n := v.(type) {
	case uint:
		val = uint64(n)
	case uint8:
		val = uint64(n)
	case uint16:
		val = uint64(n)
	case uint32:
		val = uint64(n)
	case uint64:
		val = n
	default:
		panic(fmt.Sprintf("received unknown value type: want an unsigned integer:, got %T", n))
	}
	ret := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(ret, val)
	return ret[:size]
}

func decVarUintZ(t reflect.Type, data []byte) (typex.T, error) {
	n, size := binary.Uvarint(data)
	if size <= 0 {
		return nil, fmt.Errorf("invalid varuintz encoding for: %v", data)
	}
	switch t.Kind() {
	case reflect.Uint:
		return uint(n), nil
	case reflect.Uint8:
		return uint8(n), nil
	case reflect.Uint16:
		return uint16(n), nil
	case reflect.Uint32:
		return uint32(n), nil
	case reflect.Uint64:
		return n, nil
	}
	panic(fmt.Sprintf("unreachable statement: expected an unsigned integer, got %v", t))
}

// EncodeVarUint64 encodes an uint64.
func EncodeVarUint64(value uint64, w io.Writer) error {
	var ret []byte
	for {
		// Encode next 7 bits + terminator bit
		bits := value & 0x7f
		value >>= 7

		var mask uint64
		if value != 0 {
			mask = 0x80
		}
		ret = append(ret, (byte)(bits|mask))
		if value == 0 {
			_, err := w.Write(ret)
			return err
		}
	}
}

// Variable-length encoding for integers.
//
// Takes between 1 and 10 bytes. Less efficient for negative or large numbers.
// All negative ints are encoded using 5 bytes, longs take 10 bytes. We use
// uint64 (over int64) as the primitive form to get logical bit shifts.

// TODO(herohde) 5/16/2017: figure out whether it's too slow to read one byte
// at a time here. If not, we may need a more sophisticated reader than
// io.Reader with lookahead, say.

// DecodeVarUint64 decodes an uint64.
func DecodeVarUint64(r io.Reader) (uint64, error) {
	var ret uint64
	var shift uint

	data := make([]byte, 1)
	for {
		// Get 7 bits from next byte
		if n, err := r.Read(data); n < 1 {
			return 0, err
		}

		b := data[0]
		bits := (uint64)(b & 0x7f)

		if shift >= 64 || (shift == 63 && bits > 1) {
			return 0, ErrVarIntTooLong
		}

		ret |= bits << shift
		shift += 7

		if (b & 0x80) == 0 {
			return ret, nil
		}
	}
}

// EncodeVarInt encodes an int32.
func EncodeVarInt(value int32, w io.Writer) error {
	return EncodeVarUint64((uint64)(value)&0xffffffff, w)
}

// DecodeVarInt decodes an int32.
func DecodeVarInt(r io.Reader) (int32, error) {
	ret, err := DecodeVarUint64(r)
	if err != nil {
		return 0, err
	}
	return (int32)(ret), nil
}
