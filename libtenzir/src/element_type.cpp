//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/element_type.hpp"

#include <fmt/format.h>

namespace fmt {

auto formatter<tenzir::element_type_tag>::format(
  const tenzir::element_type_tag& type, format_context& ctx) const
  -> format_context::iterator {
  return type.match(
    [&](tenzir::tag<void>) {
      return fmt::format_to(ctx.out(), "void");
    },
    [&](tenzir::tag<tenzir::chunk_ptr>) {
      return fmt::format_to(ctx.out(), "bytes");
    },
    [&](tenzir::tag<tenzir::table_slice>) {
      return fmt::format_to(ctx.out(), "events");
    });
}

} // namespace fmt
