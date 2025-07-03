#!/usr/bin/env bats
#
# SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
# SPDX-License-Identifier: BSD-3-Clause

load '../lib/bats-support/load.bash'
load '../lib/bats-assert/load.bash'
load '../lib/bats-tenzir/load.bash'

setup_file() {
    export TENZIR_PLUGINS= Bruges
}

@test "TQL format" {
    for test_file in $(find ../../tenzir/tests/format -name '*.tql'); do
        run tenzir --format "$(<"$test_file")"
        [[ "$status" -eq 0 ]]
        diff --strip-trailing-cr -u "${test_file%.*}.txt" <(echo -n "$output")
    done
}
