//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/events.hpp"

namespace tenzir::nova {

/// Selects active physical rows in `[begin, end)` by narrowing the mask.
/// Shares the data and metadata, preserving physical length and row indices.
[[nodiscard]] auto
mask_slice(Events events, storage::Index begin, storage::Index end) -> Events;

/// Rebuilds the active rows column by column, preserving their order and
/// metadata. The result contains no inactive physical rows. Batches with no
/// inactive rows are returned unchanged.
/// Use with the utmost care and only when physical materialization is really
/// necessary: this copies columns and their nested values.
[[nodiscard]] auto physically_reduce_to_mask(Events events) -> Events;

/// Selects active physical rows in `[begin, end)` and physically rebuilds them.
/// Use with the utmost care and only when physical materialization is really
/// necessary. Prefer `mask_slice` when the consumer accepts masked batches.
[[nodiscard]] auto physically_slice(Events events, storage::Index begin,
                                    storage::Index end) -> Events;

} // namespace tenzir::nova
