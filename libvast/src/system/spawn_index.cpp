//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/index.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <string_view>

namespace vast::system {

caf::expected<caf::actor>
spawn_index(node_actor::stateful_pointer<node_state> self,
            spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto opt = [&](std::string_view key, auto default_value) {
    return get_or(args.inv.options, key, default_value);
  };
  auto [archive, filesystem, accountant, catalog, type_registry]
    = self->state.registry
        .find<archive_actor, filesystem_actor, accountant_actor, catalog_actor,
              type_registry_actor>();
  if (!archive)
    return caf::make_error(ec::lookup_error, "failed to find archive actor");
  if (!filesystem)
    return caf::make_error(ec::lookup_error, "failed to find filesystem actor");
  const auto indexdir = args.dir / args.label;
  namespace sd = vast::defaults::system;
  auto partition_timeout
    = to<duration>(opt("vast.partition-timeout", sd::partition_timeout));
  if (!partition_timeout)
    return partition_timeout.error();
  auto handle = self->spawn(
    index, accountant, filesystem, archive, catalog, type_registry, indexdir,
    // TODO: Pass these options as a vast::data object instead.
    opt("vast.store-backend", std::string{sd::store_backend}),
    opt("vast.max-partition-size", sd::max_partition_size), *partition_timeout,
    opt("vast.max-resident-partitions", sd::max_in_mem_partitions),
    opt("vast.max-taste-partitions", sd::taste_partitions),
    opt("vast.max-queries", sd::num_query_supervisors),
    std::filesystem::path{opt("vast.catalog-dir", indexdir.string())},
    opt("vast.catalog-fp-rate", sd::string_synopsis_fp_rate));
  VAST_VERBOSE("{} spawned the index", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
