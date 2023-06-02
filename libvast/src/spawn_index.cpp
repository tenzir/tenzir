//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/spawn_index.hpp"

#include "vast/catalog.hpp"
#include "vast/concept/convertible/data.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/index.hpp"
#include "vast/logger.hpp"
#include "vast/node.hpp"
#include "vast/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <string_view>

namespace vast {

caf::expected<caf::actor>
spawn_index(node_actor::stateful_pointer<node_state> self,
            spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto opt = [&](std::string_view key, auto default_value) {
    return get_or(args.inv.options, key, default_value);
  };
  auto [filesystem, accountant, catalog]
    = self->state.registry
        .find<filesystem_actor, accountant_actor, catalog_actor>();
  if (!filesystem)
    return caf::make_error(ec::lookup_error, "failed to find filesystem actor");
  const auto indexdir = args.dir / args.label;
  namespace sd = vast::defaults;
  vast::index_config index_config;
  const auto* settings = get_if(&args.inv.options, "vast.index");
  if (settings) {
    vast::data as_data{};
    if (!convert(*settings, as_data))
      return caf::make_error(ec::convert_error, fmt::format("failed to convert "
                                                            "{} to data",
                                                            *settings));
    if (auto err = convert(as_data, index_config))
      return err;
    VAST_VERBOSE("using customized indexing configuration {}", index_config);
  }
  auto handle = self->spawn(
    index, accountant, filesystem, catalog, indexdir,
    // TODO: Pass these options as a vast::data object instead.
    opt("vast.store-backend", std::string{sd::store_backend}),
    opt("vast.max-partition-size", sd::max_partition_size),
    opt("vast.active-partition-timeout", sd::active_partition_timeout),
    opt("vast.max-resident-partitions", sd::max_in_mem_partitions),
    opt("vast.max-taste-partitions", sd::taste_partitions),
    opt("vast.max-queries", sd::num_query_supervisors),
    std::filesystem::path{opt("vast.catalog-dir", indexdir.string())},
    std::move(index_config));
  VAST_VERBOSE("{} spawned the index", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast
