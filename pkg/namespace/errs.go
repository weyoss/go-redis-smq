/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package namespace

import "errors"

// ErrNotFound indicates that the requested namespace does not exist.
var ErrNotFound = errors.New("namespace not found")

// ErrInvalidName indicates that the namespace name is not valid.
var ErrInvalidName = errors.New("invalid namespace name")

// ErrNameRequired indicates that the namespace name is empty.
var ErrNameRequired = errors.New("namespace name is required")

// ErrNotEmpty indicates that the namespace still contains queues or exchanges.
var ErrNotEmpty = errors.New("namespace is not empty")
