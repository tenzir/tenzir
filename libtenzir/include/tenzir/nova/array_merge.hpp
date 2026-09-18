//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/union_array.hpp"

namespace tenzir::nova {

/// Replaces rows in `old` with present rows from `new_`.
///
/// Structured arrays are merged column-wise. Only fundamental arrays
/// materialize values.
auto with_merged(MaskedArray<Array<Data>> const& old,
                 MaskedArray<Array<Data>> const& new_) -> Array<Data>;

} // namespace tenzir::nova
