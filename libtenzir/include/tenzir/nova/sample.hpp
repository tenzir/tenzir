//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/bitmap_iteration.hpp"

namespace tenzir::nova {

/// Selects at most `limit` active rows at global stride boundaries.
/// `offset` counts active rows preceding this batch in the sampling period.
inline auto sample_mask(storage::BitMap const& input, storage::Index offset,
                        storage::Index stride, uint64_t limit)
  -> storage::BitMap {
  TENZIR_ASSERT(offset >= 0 and stride > 0);
  auto result = storage::BitMap::Mutable{input.length()};
  auto phase = offset % stride;
  auto sampled = uint64_t{0};
  storage::for_each_true(input, [&](storage::Index row) {
    if (phase == 0 and sampled < limit) {
      result.set(row, true);
      ++sampled;
    }
    if (++phase == stride) {
      phase = 0;
    }
  });
  return std::move(result).finish();
}

} // namespace tenzir::nova
