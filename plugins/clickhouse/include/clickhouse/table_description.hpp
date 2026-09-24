//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "clickhouse/transformers.hpp"

#include <clickhouse/block.h>

namespace tenzir::plugins::clickhouse {

inline constexpr auto schema_refresh_interval = std::chrono::seconds{30};

struct ColumnDescription {
  std::string name;
  std::string type;
  std::string default_kind;
  std::string default_expression;
  std::string comment;

  auto operator==(ColumnDescription const&) const -> bool = default;
};
struct CachedDescription {
  std::vector<ColumnDescription> columns;
  std::chrono::steady_clock::time_point refreshed;
};
auto read_description(::clickhouse::Block const& block, diagnostic_handler& dh)
  -> failure_or<std::vector<ColumnDescription>>;
auto build_transformations(std::vector<ColumnDescription> const& description,
                           diagnostic_handler& dh)
  -> failure_or<transformer_record>;
auto refresh_transformations(transformer_record& transformations,
                             CachedDescription& cached,
                             std::vector<ColumnDescription> description,
                             std::chrono::steady_clock::time_point now,
                             diagnostic_handler& dh) -> failure_or<void>;

inline auto refresh_due(CachedDescription const& cached,
                        std::chrono::steady_clock::time_point now) -> bool {
  return now - cached.refreshed >= schema_refresh_interval;
}

} // namespace tenzir::plugins::clickhouse
