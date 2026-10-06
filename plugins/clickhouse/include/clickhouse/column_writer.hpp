//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/option.hpp"

#include <clickhouse/columns/column.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/// Writers turn columns of events into ClickHouse columns.
///
/// A writer is immutable after construction and shared by every connection.
/// Writing one insert takes two steps over whole columns. First, every column
/// narrows the mask of rows to write with `create_mask`, starting from the
/// selected events and passing its result on to the next column. Then every
/// column builds its ClickHouse column from the final mask with
/// `create_column`.
///
/// Writers see no absent values: rows without the field are nulls.
namespace tenzir::plugins::clickhouse {

/// One batch of a column: its values, and the rows of it to write.
using ColumnPart = nova::MaskedArray<nova::Array<nova::Data>>;

/// Columns mapped from a catch-all table are checked more strictly.
enum class ColumnMapping { legacy, lossless };

/// Kinds of problems that are reported once per column and insert.
enum class WriteIssue : uint8_t {
  null,
  mismatch,
  invalid,
  unsupported,
  non_object,
};

class ColumnWriter;

/// The state of writing one insert.
class WriteCtx {
public:
  explicit WriteCtx(diagnostic_handler& dh) : dh_{dh} {
  }

  /// Returns whether `writer` reports `issue` for the first time.
  auto first_report(ColumnWriter const& writer, WriteIssue issue) -> bool;

  /// Returns whether `writer` reports the unknown field `name` for the first
  /// time.
  auto first_unknown_report(ColumnWriter const& writer, std::string_view name)
    -> bool;

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

private:
  diagnostic_handler& dh_;
  std::vector<std::pair<ColumnWriter const*, uint32_t>> reported_;
  std::vector<std::pair<ColumnWriter const*, std::string>> reported_unknown_;
};

class ColumnWriter {
public:
  ColumnWriter(std::string path, std::string clickhouse_type, bool nullable)
    : path_{std::move(path)},
      clickhouse_type_{std::move(clickhouse_type)},
      nullable_{nullable} {
  }
  ColumnWriter(ColumnWriter const&) = delete;
  auto operator=(ColumnWriter const&) -> ColumnWriter& = delete;
  ColumnWriter(ColumnWriter&&) = delete;
  auto operator=(ColumnWriter&&) -> ColumnWriter& = delete;
  virtual ~ColumnWriter() = default;

  /// Returns the subset of `rows` that this column can write, and warns about
  /// the rest. Takes `rows` by value so that its storage can be reused.
  virtual auto create_mask(nova::Array<nova::Data> const& data,
                           nova::storage::BitMap rows, WriteCtx& ctx) const
    -> nova::storage::BitMap
    = 0;

  /// Builds one column from the `present` rows of all `parts`, in order. Every
  /// such row passed `create_mask`.
  virtual auto
  create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef
    = 0;

  /// The dotted path of the column, for diagnostics.
  auto path() const -> std::string_view {
    return path_;
  }

  auto clickhouse_type() const -> std::string_view {
    return clickhouse_type_;
  }

  /// Whether nulls can be written, as `NULL` or as empty structures. A `Tuple`
  /// or `Array` is nullable if its elements are.
  auto nullable() const -> bool {
    return nullable_;
  }

  /// Whether nulls are stored as `NULL`, which only a `Nullable` column can.
  auto stores_null() const -> bool;

private:
  std::string path_;
  std::string clickhouse_type_;
  bool nullable_;
};

/// Builds the writer for a column of type `clickhouse_type`, which must not
/// contain non-significant whitespace. Emits an error and returns `None` for
/// types that cannot be written.
auto make_column_writer(std::string path, std::string_view clickhouse_type,
                        ColumnMapping mapping, diagnostic_handler& dh)
  -> Option<Box<ColumnWriter>>;

/// Builds the writer for a top-level column of an unsupported type that has a
/// default. The column must be omitted, so rows that have it are rejected.
auto make_default_only_writer(std::string path, std::string clickhouse_type)
  -> Box<ColumnWriter>;

/// The type name of the value at `row`, for diagnostics.
auto kind_at(nova::Array<nova::Data> const& array, nova::storage::Index row)
  -> std::string_view;

/// An array of `length` nulls.
auto null_array(nova::storage::Index length) -> nova::Array<nova::Data>;

} // namespace tenzir::plugins::clickhouse
