//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/union_array.hpp"

#include <string>

namespace tenzir::nova {

/// Converts one row to its TQL string representation, leaving strings unquoted.
auto stringify(RowView<Data> const& row) -> std::string;

/// Converts every selected row of `array` to its TQL string representation.
/// String values remain unquoted. Rows outside `mask` have unspecified values.
auto stringify(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String>;

} // namespace tenzir::nova
