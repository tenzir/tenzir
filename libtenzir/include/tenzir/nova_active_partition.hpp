//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/nova/import_conversion.hpp"
#include "tenzir/nova_persist_result.hpp"
#include "tenzir/option.hpp"
#include "tenzir/partition_paths.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin_fwd.hpp"

#include <caf/error.hpp>

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace tenzir {

struct nova_active_partition_state {
  static constexpr auto name = "active-partition";

  nova_active_partition_actor::pointer self = nullptr;
  Option<nova::ImportConversionBuffer> conversion = None{};
  std::unordered_set<type> published;
  std::vector<active_partition_actor> children;
  partition_paths paths;
  filesystem_actor filesystem;
  caf::settings index_opts;
  index_config synopsis_opts;
  store_actor_plugin const* store_plugin = nullptr;
  std::shared_ptr<taxonomies> taxonomies;
  bool persisting = false;
};

/// Owns one shape-grouped ingestion generation and persists each resolved
/// schema as an ordinary partition.
auto nova_active_partition(
  nova_active_partition_actor::stateful_pointer<nova_active_partition_state>
    self,
  std::string name, bool internal, partition_paths paths,
  filesystem_actor filesystem, caf::settings index_opts,
  index_config synopsis_opts, store_actor_plugin const* store_plugin,
  std::shared_ptr<taxonomies> taxonomies)
  -> nova_active_partition_actor::behavior_type;

} // namespace tenzir
