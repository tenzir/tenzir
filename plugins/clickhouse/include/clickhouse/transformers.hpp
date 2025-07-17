//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/table_slice.hpp"

#include <clickhouse/columns/column.h>

namespace tenzir::plugins::clickhouse {

/// Used to represent a column name/selector. This is oftentimes modified with
/// push/pop during usage
using path_type = std::vector<std::string_view>;

using dropmask_type = std::vector<char>;
using dropmask_ref = std::span<char>;
using dropmask_cref = std::span<const char>;

inline auto pop_count(dropmask_cref mask) -> int64_t {
  return std::ranges::count(mask, true);
}

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
  /// created in `update_dropmask` and used in `create_column`.
  [[nodiscard]] virtual auto
  update_dropmask(path_type& path, const tenzir::type& type,
                  const arrow::Array& array, dropmask_ref dropmask,
                  tenzir::diagnostic_handler& dh) -> drop
    = 0;

  /// Creates a column of nulls. This is used if an output column is nullable,
  /// but not present in the input.
  [[nodiscard]] virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef = 0;

  /// Transforms an Arrow Array to a ClickHouse's Column API type, so that they
  /// can be used with the `::clickhouse::Client::Insert` function.
  /// @pre `update_dropmask` must have been called on the array
  [[nodiscard]] virtual auto
  create_column(path_type& path, const tenzir::type& type,
                const arrow::Array& array, dropmask_cref dropmask,
                int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef
    = 0;

  virtual ~transformer() = default;
};

struct transformer_record : transformer {
  using schema_transformations
    = detail::stable_map<std::string, std::unique_ptr<transformer>>;
  schema_transformations transformations;
  std::vector<char> found_column;
  const arrow::Array* my_array = nullptr;

  transformer_record() : transformer{"UNUSED", true} {};
  transformer_record(std::string clickhouse_typename,
                     schema_transformations transformations);

  virtual auto update_dropmask(path_type& path, const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh) -> drop override;

  virtual auto
  create_null_column(size_t n) const -> ::clickhouse::ColumnRef override;

  virtual auto create_column(path_type& path, const tenzir::type& type,
                             const arrow::Array& array, dropmask_cref dropmask,
                             int64_t dropcount, tenzir::diagnostic_handler& dh)
    -> ::clickhouse::ColumnRef override;

  struct find_result {
    transformer* trafo;
    ssize_t index;
  };
  auto transfrom_and_index_for(std::string_view name) const -> find_result;
};

auto remove_non_significant_whitespace(std::string_view str) -> std::string;

auto type_to_clickhouse_typename(path_type& path, tenzir::type t, bool nullable,
                                 diagnostic_handler& dh)
  -> failure_or<std::string>;

auto plain_clickhouse_tuple_elements(path_type& path, const record_type& record,
                                     diagnostic_handler& dh,
                                     std::string_view primary = "")
  -> failure_or<std::string>;

auto make_functions_from_clickhouse(path_type& path,
                                    const std::string_view clickhouse_typename,
                                    diagnostic_handler&)
  -> std::unique_ptr<transformer>;

} // namespace tenzir::plugins::clickhouse
