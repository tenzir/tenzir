//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/client.h"
#include "tenzir/diagnostics.hpp"
#include "tenzir/type.hpp"

#include "arguments.hpp"
#include "transformers.hpp"

namespace tenzir::plugins::clickhouse {
class Easy_Client {
public:
  explicit Easy_Client(Arguments args, diagnostic_handler& dh)
    : client_{args.make_options()}, args_{std::move(args)}, dh_{dh} {
  }

  static auto
  make(Arguments args, diagnostic_handler& dh) -> std::unique_ptr<Easy_Client>;

  auto insert(const table_slice& slice) -> bool;

private:
  auto table_exists() -> bool;
  auto get_schema_transformations() -> bool;
  auto create_table(const tenzir::record_type& schema) -> bool;

private:
  ::clickhouse::Client client_;
  Arguments args_;
  diagnostic_handler& dh_;
  std::optional<transformer_record> transformations_;
  dropmask_type dropmask_;
};

} // namespace tenzir::plugins::clickhouse
