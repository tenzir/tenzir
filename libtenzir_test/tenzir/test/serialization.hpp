//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/json_reader.hpp>
#include <caf/json_writer.hpp>

namespace tenzir {

/// Calls `inspect(f, x)`, checks the result and potentially prints the error.
void inspect_or_error(auto& f, auto& x) {
  auto result = f.apply(x);
  if (!result) {
    MESSAGE("error: " << f.get_error());
  }
  CHECK(result);
}

void try_check_equal(auto&& x, auto&& y) {
  if constexpr (requires { x == y; }) {
    CHECK_EQUAL(x, y);
  }
}

template <class T>
void check_binary_serialization(const T& x) {
  MESSAGE("before = " << x);
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{nullptr, buffer};
  inspect_or_error(serializer, x);
  MESSAGE("binary = " << buffer);
  auto deserializer = caf::binary_deserializer{nullptr, buffer};
  auto y = T{};
  inspect_or_error(deserializer, y);
  MESSAGE("after = " << y);
  try_check_equal(x, y);
}

template <class T>
void check_json_serialization(const T& x) {
  MESSAGE("before = " << x);
  auto serializer = caf::json_writer{};
  inspect_or_error(serializer, x);
  auto deserializer = caf::json_reader{};
  MESSAGE("json = " << serializer.str());
  deserializer.load(serializer.str());
  auto y = T{};
  inspect_or_error(deserializer, y);
  MESSAGE("after = " << y);
  try_check_equal(x, y);
}

template <class T>
void check_serialization(const T& x) {
  check_binary_serialization(x);
  check_json_serialization(x);
}

} // namespace tenzir
