//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/tag.hpp>

namespace tenzir {

/// Describes the input or output type of an operator.
struct element_type_tag : tag_variant<void, table_slice, chunk_ptr> {
  using tag_variant::tag_variant;
};

/// Describes the input and output type of an operator.
struct element_type_tag_pair {
  element_type_tag input;
  element_type_tag output;
};

/// The list of all valid element types.
using element_types = element_type_tag::types;

/// A concept for valid input and output types of an operator.
template <class T>
concept element_type = detail::tl_contains_v<element_types, T>;

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::element_type_tag> {
  constexpr auto parse(format_parse_context ctx) {
    return ctx.begin();
  }

  auto format(const tenzir::element_type_tag& type, format_context& ctx) const
    -> format_context::iterator;
};

} // namespace fmt
