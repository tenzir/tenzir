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

namespace tenzir::nova {

/// Computes a stable structural type fingerprint for every selected row.
/// Rows outside `mask` have unspecified values.
auto type_id(Array<Data> const& array, storage::BitMap const& mask)
  -> Array<String>;

} // namespace tenzir::nova
