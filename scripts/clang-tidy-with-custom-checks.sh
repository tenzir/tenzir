#!/usr/bin/env bash

set -euo pipefail

exec clang-tidy --experimental-custom-checks "$@"
