// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !noasm
// +build !noasm

package utils_test

import (
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v10/internal/utils"
)

var (
	src     = make([]int8, 1000)
	mapping = make([]int32, 50)
)

func init() {
	for i := range mapping {
		mapping[i] = int32(i * 100)
	}

	for i := range src {
		src[i] = int8(rand.Intn(50))
	}
}

func BenchmarkTransposeASM(b *testing.B) {
	dest := make([]int64, len(src))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		utils.TransposeInts(src, dest, mapping)
	}
}
