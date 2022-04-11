//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE pipeline

#include "vast/pipeline.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/pipeline.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

template <class T>
pipeline to_pipeline(const T& x) {
  return unbox(to<pipeline>(x));
}

} // namespace

TEST(empty pipeline) {
  auto p = to_pipeline("foo == 42");
  fmt::print("{}\n", p);
}

TEST(sequence) {
  auto p = to_pipeline("foo == 42 | do");
  fmt::print("{}\n", p);
  p = to_pipeline("foo == 42 | x --opt | y -o | z");
  fmt::print("{}\n", p);
}
