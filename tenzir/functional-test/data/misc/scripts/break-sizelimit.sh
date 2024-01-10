#!/usr/bin/env bash

set -eu

partition="$(find "${TENZIR_DB_DIRECTORY}/index/" -name '*.mdx' | head -1)"
dd if=/dev/zero bs=1G seek=3 count=0 of="${partition}"
