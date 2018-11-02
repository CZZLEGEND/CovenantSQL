/*
 * Copyright 2018 The CovenantSQL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package log

import "github.com/pkg/errors"

var (
	// ErrPoolClosed represents the log file is closed.
	ErrPoolClosed = errors.New("log is closed")
	// ErrInvalidLog represents the log object is invalid.
	ErrInvalidLog = errors.New("invalid log")
	// ErrTruncated represents the log exists but truncated.
	ErrTruncated = errors.New("log truncated")
	// ErrAlreadyExists represents the log already exists.
	ErrAlreadyExists = errors.New("log already exists")
	// ErrNotExists represents the log does not exists.
	ErrNotExists = errors.New("log not exists")
)