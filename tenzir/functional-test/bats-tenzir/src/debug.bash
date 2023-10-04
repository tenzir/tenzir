# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

# Prints the given parameters.
debug() {
  local level=$1
  shift
  if [ "$level" -le "${debug_level:-0}" ]; then
    echo "# $*" >&3
  fi
}

