//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

namespace tenzir {

struct operator_index {
  // Position in the parent pipeline.
  uint64_t position = {};
  // id fragment used to identify an instantiated subpipeline.
  // TODO: consider using a data instead.
  std::string id_fragment = {};

  friend auto inspect(auto& f, operator_index& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.operator_index")
      .fields(f.field("position", x.position),
              f.field("id_fragment", x.id_fragment));
  }
};

// Also defined in fwd.hpp.
using pipeline_path = std::vector<operator_index>;

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::operator_index> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::operator_index& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "[{}:{}]", x.position, x.id_fragment);
  }
};

} // namespace fmt
