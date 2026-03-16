//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir {

/// Clears the global evaluation cache for constant-folded series.
void clear_eval_cache();

/// Converts a data value to a series with caching. Looks up the value in a
/// global cache and reuses the cached series if available and long enough.
auto cached_data_to_series(const data& x, int64_t length) -> series;

} // namespace tenzir
