//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ids.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("make ids") {
  ids xs;
  xs.append_bit(false);
  xs.append_bit(true);
  xs.append_bit(true);
  xs.append_bits(false, 7);
  xs.append_bits(true, 10);
  auto ys = make_ids({1, 2, {10, 20}});
  CHECK_EQUAL(xs, ys);
  auto zs = make_ids({{15, 20}, 2, {10, 15}, 1});
  CHECK_EQUAL(ys, zs);
}
