#!/usr/bin/env bash

#
# Copyright (c) 2026
# Weyoss <weyoss@outlook.com>
# https://github.com/weyoss
#
# This source code is licensed under the MIT license found in the LICENSE file
# in the root directory of this source tree.
#
#

set -euo pipefail

#
# Syncs Lua scripts from the TypeScript RedisSMQ implementation into the
# Go implementation's script directory.
#
# The source repository is expected to be checked out as a sibling directory:
#   ../redis-smq

SOURCE_ROOT="../redis-smq/packages/redis-smq/src/common/redis/scripts"
JOBS_SOURCE="../redis-smq/packages/redis-smq/src/common/abstract/background-job/redis/scripts"
DEST_CORE="internal/redis/scripts/scripts/core"
DEST_JOBS="internal/redis/scripts/scripts/jobs"

# ---------------------------------------------------------------------------
# Helper: copy scripts from $1 (source dir) to $2 (destination dir).
# ---------------------------------------------------------------------------
copy_scripts() {
    local src="$1"
    local dst="$2"

    if [ ! -d "$src" ]; then
        echo "ERROR: source directory not found: $src" >&2
        echo "Make sure the redis-smq TypeScript repo is checked out as a sibling." >&2
        exit 1
    fi

    echo "Syncing $dst ..."
    rm -rf "$dst"
    mkdir -p "$dst"
    cp -r "$src"/* "$dst/"
    echo "done"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=== RedisSMQ Lua script sync ==="
copy_scripts "$SOURCE_ROOT" "$DEST_CORE"
copy_scripts "$JOBS_SOURCE" "$DEST_JOBS"
echo "=== Sync complete ==="