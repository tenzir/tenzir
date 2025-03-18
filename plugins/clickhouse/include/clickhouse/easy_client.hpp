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

class easy_client {
public:
  explicit easy_client(arguments args, diagnostic_handler& dh)
    : client_{args.make_options()}, args_{std::move(args)}, dh_{dh} {
  }

  static auto
  make(arguments args, diagnostic_handler& dh) -> std::unique_ptr<easy_client>;

  auto insert(const table_slice& slice) -> failure_or<void>;

private:
  auto table_exists() -> bool;
  auto get_schema_transformations() -> failure_or<void>;
  auto create_table(const tenzir::record_type& schema) -> failure_or<void>;

private:
  ::clickhouse::Client client_;
  arguments args_;
  diagnostic_handler& dh_;
  std::optional<transformer_record> transformations_;
  dropmask_type dropmask_;
};

} // namespace tenzir::plugins::clickhouse
