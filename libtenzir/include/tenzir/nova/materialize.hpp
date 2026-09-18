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

/// Converts a single nova row into an owning legacy `data` value. Strings are
/// copied, records keep their field order, and lists are converted
/// element-wise.
auto materialize(const RowView<Data>& row) -> data;

} // namespace tenzir::nova
