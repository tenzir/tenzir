//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <fmt/format.h>

namespace tenzir {

/// The type of a layer-2 frame.
enum class frame_type : uint16_t {
  invalid = 0, // DLT_NULL
  ethernet = 1, // DLT_EN10MB
  sll2 = 276, // DLT_LINUX_SLL2
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::frame_type> : formatter<string_view> {
  using super = formatter<string_view>;

  template <class FormatContext>
  constexpr auto format(const tenzir::frame_type& x, FormatContext& ctx)
    -> decltype(ctx.out()) {
    switch (x) {
      case tenzir::frame_type::invalid:
        return super::format("invalid", ctx);
      case tenzir::frame_type::ethernet:
        return super::format("ethernet", ctx);
      case tenzir::frame_type::sll2:
        return super::format("sll2", ctx);
    }
    return super::format("unknown", ctx);
  }
};

} // namespace fmt
