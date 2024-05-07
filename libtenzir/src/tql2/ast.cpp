//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/ast.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace tenzir::ast {

auto expression::get_location() const -> location {
  return match([](const auto& x) {
    return x.get_location();
  });
}

auto expression::copy() const -> expression {
  // TODO: Consider making this just copyable.
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{nullptr, buffer};
  auto success = inspect(serializer, const_cast<expression&>(*this));
  TENZIR_ASSERT(success);
  auto deserializer = caf::binary_deserializer{nullptr, buffer};
  auto result = expression{};
  success = inspect(deserializer, result);
  TENZIR_ASSERT(success);
  return result;
}

} // namespace tenzir::ast
