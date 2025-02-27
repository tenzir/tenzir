//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/client.h"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/type.hpp"

#include <string_view>

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
  /// Note that while `Tuple(Ts..)` and `Array(T)` themselfs are not nullable,
  /// their respective transformers may be, iff all value types in there are
  /// nullable.
  bool clickhouse_nullable;

  transformer(std::string clickhouse_typename, bool clickhouse_nullable)
    : clickhouse_typename{std::move(clickhouse_typename)},
      clickhouse_nullable{clickhouse_nullable} {
  }

  /// This function updates a `dropmask`. Events where the dropmask is true
  /// shall be dropped from the output, as they they contain null values for
  /// non-nullable columns in ClickHouse.
  /// This function is not `const`, as the array version holds state that is
  /// created in `update_dropmask` and used in `create_columns`.
  [[nodiscard]] virtual auto
  update_dropmask(const tenzir::type& type, const arrow::Array& array,
                  dropmask_ref dropmask, tenzir::diagnostic_handler& dh) -> bool
                                                                            = 0;

  /// Transforms an Arrow Array to a ClickHouse's Column API type, so that they
  /// can be used with the `::clickhouse::Client::Insert` function.
  [[nodiscard]] virtual auto create_columns(
    const tenzir::type& type, const arrow::Array& array, dropmask_cref dropmask,
    tenzir::diagnostic_handler& dh) const -> ::clickhouse::ColumnRef = 0;

  virtual ~transformer() = default;
};

using schema_transformations
  = std::unordered_map<std::string, std::unique_ptr<transformer>,
                       detail::heterogeneous_string_hash,
                       detail::heterogeneous_string_equal>;

struct Easy_Client {
  ::clickhouse::Client client;
  location operator_location;
  diagnostic_handler& dh;

  explicit Easy_Client(::clickhouse::ClientOptions opts, location loc,
                       diagnostic_handler& dh)
    : client{std::move(opts)}, operator_location{loc}, dh{dh} {
  }

  auto table_exists(std::string_view table) -> bool;

  auto get_schema_transformations(std::string_view table)
    -> std::optional<schema_transformations>;

  auto create_table(std::string_view table, std::string_view primary,
                    const tenzir::record_type& schema)
    -> std::optional<schema_transformations>;
};
} // namespace tenzir::plugins::clickhouse
