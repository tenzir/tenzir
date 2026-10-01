//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"

#include <string>

namespace tenzir::nova {

/// Converts one row to its TQL string representation, leaving strings unquoted.
/// Returns `None` for a top-level blob with invalid UTF-8; a null row yields
/// the string `null`.
auto stringify(RowView<Data> const& row) -> Option<std::string>;

/// Converts every selected row of `array` to its TQL string representation.
/// Strings remain unquoted and top-level blobs are decoded as UTF-8. Nulls
/// and invalid blobs render as `null`. Rows outside `mask` are unspecified.
auto stringify(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String>;

/// Like the overload above, but warns at `source` for invalid UTF-8 blobs.
/// The presence mask excludes nulls and invalid blobs, while their backing
/// string remains `null` for interpolation.
auto stringify(Array<Data> const& array, storage::BitMap const& mask,
               location source, diagnostic_handler& dh)
  -> MaskedArray<Array<String>>;

/// Converts every selected row of `array` to JSON with the given printer
/// `options`. Unlike the TQL representation above, every value goes through
/// the printer, so strings are quoted and nulls print as `null`. Rows outside
/// `mask` are unspecified.
auto stringify(Array<Data> const& array, storage::BitMap const& mask,
               json_printer_options options) -> Array<String>;

} // namespace tenzir::nova
