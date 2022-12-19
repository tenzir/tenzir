//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/die.hpp"

#include <fmt/format.h>

namespace vast {

/// The possible encodings of a table slice.
/// @note This encoding is unversioned. Newly created table slices are
/// guaranteed to use the newest vesion of the encoding, while deserialized
/// table slices may use an older version.
enum class table_slice_encoding : uint8_t {
  none,         ///< No data is encoded; the table slice is empty or invalid.
  arrow,        ///< The table slice is encoded using the Apache Arrow format.
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::table_slice_encoding> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const vast::table_slice_encoding& x, FormatContext& ctx) const {
    switch (x) {
      case vast::table_slice_encoding::none:
        return format_to(ctx.out(), "none");
      case vast::table_slice_encoding::arrow:
        return format_to(ctx.out(), "arrow");
    }
    vast::die("unreachable");
  }
};

} // namespace fmt
