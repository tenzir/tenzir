//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/restore.hpp"

#include <tenzir/test/test.hpp>

namespace tenzir::plugins::iceberg {

namespace {

TEST("fresh runs never claim an existing table in create mode") {
  CHECK(not may_resume_existing_table({
    .restored = false,
  }));
}

TEST("an empty restored checkpoint does not claim an existing table") {
  // A checkpoint can be taken before the first input creates the table; an
  // externally created table with the same name must still raise the
  // create-mode conflict after such a restore.
  CHECK(not may_resume_existing_table({
    .restored = true,
  }));
}

TEST("a restored checkpoint that created the table resumes it") {
  CHECK(may_resume_existing_table({
    .restored = true,
    .created_table = true,
  }));
}

TEST("a restored checkpoint with committed epochs resumes the table") {
  CHECK(may_resume_existing_table({
    .restored = true,
    .commit_seq = 1,
  }));
}

TEST("a restored checkpoint with staged files resumes the table") {
  CHECK(may_resume_existing_table({
    .restored = true,
    .restored_files = 3,
  }));
}

TEST("creation evidence alone does not bypass conflicts on fresh runs") {
  CHECK(not may_resume_existing_table({
    .restored = false,
    .created_table = true,
    .commit_seq = 2,
    .restored_files = 1,
  }));
}

TEST("a missing table is benign before the first write") {
  CHECK(not missing_table_is_fatal({
    .restored = true,
    .created_table = true,
  }));
}

TEST("a missing table is fatal when the checkpoint holds file handles") {
  CHECK(missing_table_is_fatal({
    .restored = true,
    .restored_files = 2,
  }));
}

TEST("a missing table is fatal after committed epochs") {
  // Committed rows live only in the table; recreating a fresh table from
  // post-checkpoint input would silently lose them.
  CHECK(missing_table_is_fatal({
    .restored = true,
    .created_table = true,
    .commit_seq = 4,
  }));
}

} // namespace

} // namespace tenzir::plugins::iceberg
