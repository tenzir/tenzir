//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/client.h"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/type.hpp"

#include <string_view>

namespace tenzir::plugins::clickhouse {
using dropmask_ref = std::span<char>;

struct transformer {
  std::string clickhouse_typename;

  transformer(std::string clickhouse_typename)
    : clickhouse_typename{std::move(clickhouse_typename)} {
  }

  virtual auto update_dropmask(const tenzir::type& type,
                               const arrow::Array& array, dropmask_ref dropmask,
                               tenzir::diagnostic_handler& dh) -> bool = 0;

  virtual auto
  create_columns(const tenzir::type& type, const arrow::Array& array,
                 dropmask_ref dropmask,
                 tenzir::diagnostic_handler& dh) -> ::clickhouse::ColumnRef = 0;

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

  auto create_table(std::string_view table, const tenzir::record_type& schema)
    -> std::optional<schema_transformations>;
};
} // namespace tenzir::plugins::clickhouse
