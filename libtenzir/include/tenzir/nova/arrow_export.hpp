//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/result.hpp"
#include "tenzir/table_slice.hpp"

#include <span>
#include <vector>

namespace tenzir::nova {

/// Converts selected rows column by column using an already discovered schema.
/// Nulls may inhabit any target type; concrete types and record shapes must
/// match. The schema supplies the output name and attributes.
auto to_arrow_record_batch(Array<Record> const& records, type const& schema,
                           std::span<storage::Index const> rows)
  -> Result<std::shared_ptr<arrow::RecordBatch>, std::string>;

/// Converts selected Nova events to Arrow-backed table slices, grouping by
/// schema name and internal status and splitting incompatible row shapes.
/// Import times are not exported as schema metadata.
auto to_table_slices(Events const& events) -> std::vector<table_slice>;

/// A single-schema export stream. Only null types (including list children)
/// may be refined during discovery. Field sets, order, and metadata are fixed.
/// Limits bound discovery, not the size of the returned output. One individual
/// row may exceed the byte limit. Import times are not schema metadata.
class ArrowExportBuilder {
public:
  struct Limits {
    size_t rows = 10'000;
    size_t bytes = 16 * 1024 * 1024;
  };

  ArrowExportBuilder();
  explicit ArrowExportBuilder(Limits limits);

  auto add(Events const& events, diagnostic_handler& dh, location loc)
    -> failure_or<std::vector<table_slice>>;
  auto finish(diagnostic_handler& dh, location loc)
    -> failure_or<std::vector<table_slice>>;

private:
  auto accept(table_slice slice, std::vector<table_slice>& output,
              diagnostic_handler& dh, location loc) -> failure_or<void>;
  auto flush(std::vector<table_slice>& output, diagnostic_handler& dh,
             location loc) -> failure_or<void>;

  Limits limits_;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<table_slice> pending_;
  size_t rows_ = 0;
  size_t bytes_ = 0;
  bool fixed_ = false;
  bool limit_committed_ = false;
};

} // namespace tenzir::nova
