//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/nova/union_array.hpp"

namespace tenzir::nova {

/// Converts a single nova row into an owning Nova `Data` value.
auto materialize_data(const RowView<Data>& row) -> Data;

/// Converts a single nova row into an owning legacy `data` value at an explicit
/// legacy boundary. Strings are copied, records keep their field order, and
/// lists are converted element-wise.
auto materialize_legacy(const RowView<Data>& row) -> data;

/// Converts an owning Nova `Data` value into a legacy `data` value, like the
/// overload above.
auto materialize_legacy(const Data& value) -> data;

} // namespace tenzir::nova
