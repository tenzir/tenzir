//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/columns/column.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/table_slice.hpp"

// TODO: This should go into table_slice.hpp or somewhere like that
namespace tenzir {
struct column_view {
  std::string_view name;
  const tenzir::type& type;
  const arrow::Array& array;
};

inline auto columns_of(const table_slice& slice) -> generator<column_view> {
  const auto& schema = as<record_type>(slice.schema());
  for (auto [i, kt] : detail::enumerate(schema.fields())) {
    const auto& [k, _] = kt;
    auto offset = tenzir::offset{};
    offset.push_back(i);
    auto [t, arr] = offset.get(slice);
    co_yield column_view{k, t, *arr};
  }
}

inline auto
columns_of(const record_type& schema,
           const arrow::StructArray& array) -> generator<column_view> {
  for (auto [i, kt] : detail::enumerate(schema.fields())) {
    const auto& [k, t] = kt;
    auto offset = tenzir::offset{};
    offset.push_back(i);
    auto arr = offset.get(array);
    co_yield column_view{k, t, *arr};
  }
}
} // namespace tenzir

namespace tenzir::plugins::clickhouse {

  using dropmask_type = std::vector<char>;

using dropmask_ref = std::span<char>;
using dropmask_cref = std::span<const char>;

/// A `transformer` performs the type erased conversion from our arrays to
/// clickhouse-cpp's API types
struct transformer {
  /// The name of the resulting type in ClickHouse
  std::string clickhouse_typename;
  /// Whether the "column" in clickhouse would be nullable.
  /// Note that while `Tuple(Ts..)` and `Array(T)` themselves are not nullable
  /// in ClickHouse, the nested types may be.
  /// Iff all nested columns are nullable, we consider the Tuple/Array nullable
  /// as well.
  bool clickhouse_nullable;

  transformer(std::string clickhouse_typename, bool clickhouse_nullable)
    : clickhouse_typename{std::move(clickhouse_typename)},
      clickhouse_nullable{clickhouse_nullable} {
  }

  enum class drop { none, some, all };

  friend auto operator|(drop lhs, drop rhs) -> drop {
    if (lhs == rhs) {
      return lhs;
    }
    if (lhs == drop::all or rhs == drop::all) {
      return drop::all;
    }
    return drop::some;
  }

  /// This function updates a `dropmask`. Events where the dropmask is true
  /// shall be dropped from the output, as they they contain null values for
  /// non-nullable columns in ClickHouse.
  /// This function is not `const`, as the array version holds state that is
  /// created in `update_dropmask` and used in `create_columns`.
  [[nodiscard]] virtual auto
  update_dropmask(const tenzir::type& type, const arrow::Array& array,
                  dropmask_ref dropmask, tenzir::diagnostic_handler& dh) -> drop
                                                                            = 0;

  /// Creates a column of nulls. This is used if an output column is nullable,
  /// but not present in the input.
  [[nodiscard]] virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef = 0;

  /// Transforms an Arrow Array to a ClickHouse's Column API type, so that they
  /// can be used with the `::clickhouse::Client::Insert` function.
  [[nodiscard]] virtual auto
  create_column(const tenzir::type& type, const arrow::Array& array,
                dropmask_cref dropmask,
                tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef
                                                         = 0;

  virtual ~transformer() = default;
};

struct transformer_record : transformer {
  using schema_transformations
    = detail::stable_map<std::string, std::unique_ptr<transformer>>;
  schema_transformations transformations;
  std::vector<char> found_column;

  transformer_record() : transformer{"UNUSED", true} {};
  transformer_record(std::string clickhouse_typename,
                     schema_transformations transformations);

  virtual auto update_dropmask(const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh) -> drop override;

  virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef override;

  virtual auto create_column(
    const tenzir::type& type, const arrow::Array& array, dropmask_cref dropmask,
    tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef override;

  struct find_result {
    transformer* trafo;
    ssize_t index;
  };
  auto transfrom_and_index_for(std::string_view name) const -> find_result;
};

auto remove_non_significant_whitespace(std::string_view str) -> std::string;

auto type_to_clickhouse_typename(tenzir::type t, bool nullable) -> std::string;

auto plain_clickhouse_tuple_elements(const record_type& record,
                                     std::string_view primary
                                     = "") -> std::string;

auto make_functions_from_clickhouse(const std::string_view clickhouse_typename)
  -> std::unique_ptr<transformer>;
}
