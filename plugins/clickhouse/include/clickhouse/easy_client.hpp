//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/arguments.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/type.hpp"

#include <clickhouse/client.h>

namespace tenzir::plugins::clickhouse {

auto inline has_location(const diagnostic& diag) -> bool {
  for (const auto& a : diag.annotations) {
    if (a.source != location::unknown) {
      return true;
    }
  }
  return false;
}

class easy_client {
public:
  explicit easy_client(arguments args, diagnostic_handler& dh)
    : client_{args.make_options()},
      args_{std::move(args)},
      dh_{dh, [loc = args.operator_location](diagnostic diag) -> diagnostic {
            if (not has_location(diag)) {
              diag.annotations.emplace_back(true, std::string{}, loc);
            }
            return diag;
          }} {
  }

  static auto
  make(arguments args, diagnostic_handler& dh) -> std::unique_ptr<easy_client>;

  auto insert(const table_slice& slice) -> bool;

private:
  auto check_if_table_exists() -> bool;
  auto get_schema_transformations() -> failure_or<void>;
  auto create_table(const tenzir::record_type& schema) -> failure_or<void>;

private:
  ::clickhouse::Client client_;
  arguments args_;
  transforming_diagnostic_handler dh_;
  std::optional<transformer_record> transformations_;
  dropmask_type dropmask_;
};

} // namespace tenzir::plugins::clickhouse
