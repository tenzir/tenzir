//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concepts.hpp"
#include "tenzir/test/test.hpp"

#include <array>
#include <cstddef>
#include <span>

using namespace tenzir;
using namespace tenzir::concepts;

namespace {

struct invalid {
  friend std::span<const int> as_bytes(invalid) {
    return {};
  };
};

struct variable {
  friend std::span<const std::byte> as_bytes(const variable& x) {
    return std::span{x.bytes};
  };

  std::array<std::byte, 42> bytes;
};

struct fixed {
  friend std::span<const std::byte, 42> as_bytes(const fixed& x) {
    return std::span{x.bytes};
  };

  std::array<std::byte, 42> bytes;
};

} // namespace

static_assert(!byte_sequence<invalid>);
static_assert(byte_sequence<fixed>);
static_assert(byte_sequence<variable>);
static_assert(!variable_byte_sequence<fixed>);
static_assert(variable_byte_sequence<variable>);
static_assert(fixed_byte_sequence<fixed>);
static_assert(!fixed_byte_sequence<variable>);

TEST("byte sequences") {
  CHECK(as_bytes(invalid{}).empty());
  CHECK_EQUAL(as_bytes(fixed{}).size(), 42u);
  CHECK_EQUAL(as_bytes(variable{}).size(), 42u);
}
