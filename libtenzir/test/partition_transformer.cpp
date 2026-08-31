//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/partition_transformer.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("partition reduction") {
  CHECK(satisfies_partition_reduction(6, 1, 5, 0));
  CHECK(not satisfies_partition_reduction(5, 1, 5, 0));
  CHECK(satisfies_partition_reduction(3, 1, 0, 0.6));
  CHECK(not satisfies_partition_reduction(2, 1, 0, 0.6));
  CHECK(satisfies_partition_reduction(5, 2, 0, 0.6));
  CHECK(not satisfies_partition_reduction(5, 3, 0, 0.6));
  CHECK(satisfies_partition_reduction(1, 1, 0, 0));
}
