#!/usr/bin/env bash

set -eu

mdx="$(find "${VAST_INTEGRATION_DB_DIRECTORY}/index/" -name '*.mdx')"
partition="${mdx%.*}"
dd if=/dev/zero bs=1G seek=3 count=0 of="${partition}"
