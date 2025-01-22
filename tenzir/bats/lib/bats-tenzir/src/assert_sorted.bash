# SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

assert_sorted() {
  local output_="$output"
  output=$(LC_ALL=C sort <<< "$output_")
  assert_output "$@"
}

