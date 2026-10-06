//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "clickhouse/transformers.hpp"

namespace tenzir::plugins::clickhouse {

auto prepare_slice(table_slice const& slice, transformer_record const& tr,
                   diagnostic_handler& dh, location operator_location)
  -> table_slice;

// Extract mapped paths and pack the remaining record into the catch-all.
// Value validation and JSON serialization belong to the existing writer.
auto restructure_for_catch_all(table_slice const& slice,
                               transformer_record const& tr) -> table_slice;

/// A part of a slice that `split_null_defaults` produced.
struct DefaultedPart {
  table_slice slice;
  /// The rows of the input slice in `slice`, or empty if it has all of them.
  std::vector<int64_t> rows;
};

/// Splits `slice` so that the rows of a part are `null` for the same set of
/// non-`Nullable` columns with a default. Each part omits these columns, so
/// that ClickHouse fills in their defaults, just as for events without the
/// field.
auto split_null_defaults(table_slice const& slice, transformer_record const& tr)
  -> std::vector<DefaultedPart>;

} // namespace tenzir::plugins::clickhouse
