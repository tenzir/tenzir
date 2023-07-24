//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/die.hpp"

#include <fmt/format.h>

namespace tenzir {

/// The possible encodings of a table slice.
/// @note This encoding is unversioned. Newly created table slices are
/// guaranteed to use the newest vesion of the encoding, while deserialized
/// table slices may use an older version.
enum class table_slice_encoding : uint8_t {
  none,  ///< No data is encoded; the table slice is empty or invalid.
  arrow, ///< The table slice is encoded using the Apache Arrow format.
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::table_slice_encoding> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::table_slice_encoding& x, FormatContext& ctx) const {
    switch (x) {
      case tenzir::table_slice_encoding::none:
        return fmt::format_to(ctx.out(), "none");
      case tenzir::table_slice_encoding::arrow:
        return fmt::format_to(ctx.out(), "arrow");
    }
    tenzir::die("unreachable");
  }
};

} // namespace fmt
