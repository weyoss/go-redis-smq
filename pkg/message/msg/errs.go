/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package msg

import "errors"

// ErrNotFound indicates that the requested message does not exist.
var ErrNotFound = errors.New("message not found")

// ErrNotRequeuable indicates that the message cannot be requeued because
// its current status is not Acknowledged or DeadLettered.
var ErrNotRequeuable = errors.New("message is not requeuable")
