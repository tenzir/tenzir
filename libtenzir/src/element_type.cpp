//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/element_type.hpp"

#include "tenzir/nova_flag.hpp"

#include <fmt/format.h>

namespace tenzir {

auto events_element_type() -> element_type_tag {
  if (nova_enabled()) {
    return tag_v<nova::Events>;
  }
  return tag_v<table_slice>;
}

} // namespace tenzir

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
    },
    [&](tenzir::tag<tenzir::nova::Events>) {
      return fmt::format_to(ctx.out(), "nova_events");
    },
    [&](tenzir::tag<tenzir::FileHandle>) {
      return fmt::format_to(ctx.out(), "file");
    });
}

} // namespace fmt
