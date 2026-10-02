//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/column_writer.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::clickhouse {

template <class Base, class T, class... Args>
auto make_boxed(Args&&... args) -> Box<Base> {
  return Box<Base>::from_non_null(
    std::make_unique<T>(std::forward<Args>(args)...));
}

/// Calls `f(row, value)` for the set bits of `rows`, in order, with the
/// storage of `array` matched once.
template <class Tag, class F>
auto for_each_value(nova::Array<Tag> const& array,
                    nova::storage::BitMap const& rows, F&& f) -> void {
  match(array.storage(), [&](auto const& storage) {
    for (auto row : nova::storage::true_bits(rows)) {
      f(row, storage.get(row));
    }
  });
}

/// The rows of `data` that are null.
auto null_rows(nova::Array<nova::Data> const& data) -> nova::storage::BitMap;

/// The first set bit of `rows`, which must have one.
auto first_row(nova::storage::BitMap const& rows) -> nova::storage::Index;

/// The total number of set bits over all parts.
auto total_rows(std::span<ColumnPart const> parts) -> size_t;

/// Wraps `nested` into a `Nullable` column if there are `nulls`.
auto wrap_nullable(::clickhouse::ColumnRef nested,
                   Option<std::vector<uint8_t>> nulls)
  -> ::clickhouse::ColumnRef;

// Diagnostics shared by the writers. Each is emitted once per column and insert.

auto report_null(ColumnWriter const& writer, WriteCtx& ctx) -> void;

/// Reports the type of the first row of `rows` in `data`.
auto report_mismatch(ColumnWriter const& writer, std::string_view expected,
                     nova::Array<nova::Data> const& data,
                     nova::storage::BitMap const& rows, WriteCtx& ctx) -> void;

auto report_unsupported(ColumnWriter const& writer, std::string_view note,
                        WriteCtx& ctx) -> void;

// Factories for the leaf writers in `leaf_writers.cpp`.

/// Returns `None` if `type` is not a scalar type. `type` excludes the
/// `Nullable` wrapper.
auto make_scalar_writer(std::string path, std::string_view type, bool nullable,
                        ColumnMapping mapping) -> Option<Box<ColumnWriter>>;

auto make_subnet_writer(std::string path, std::string type, bool nullable)
  -> Box<ColumnWriter>;

auto make_json_writer(std::string path, std::string type, bool sql_nullable)
  -> Box<ColumnWriter>;

} // namespace tenzir::plugins::clickhouse
