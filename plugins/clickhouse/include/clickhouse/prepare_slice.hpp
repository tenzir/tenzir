// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once
#include "clickhouse/transformers.hpp"

namespace tenzir::plugins::clickhouse {
/// Rewrites `slice` for insertion into the target table described by `tr`:
/// - fields that target a ClickHouse `JSON` column, at any nesting depth
///   (top-level or nested inside records/lists), are replaced with their
///   opaque JSON-string rendering (a field already of type `string` is left
///   unchanged, assumed to be JSON);
/// - top-level columns the table does not accept (unknown, or generated
///   `MATERIALIZED`/`ALIAS` columns) are dropped with a warning.
///
/// Dropping table-absent columns here — before the by-schema grouping in
/// `insert_table` — lets events that differ only in such columns collapse
/// into one schema and batch together. Pre-serializing JSON fields at any
/// depth (not just top-level) similarly lets events that differ only in the
/// shape of a nested JSON-bound field (e.g. OCSF's `file.xattributes`)
/// collapse into one schema.
auto prepare_slice(const table_slice& slice, const transformer_record& tr,
                   diagnostic_handler& dh, location operator_location)
  -> table_slice;
} // namespace tenzir::plugins::clickhouse
