//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_index.hpp"

#include "tenzir/catalog.hpp"
#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/error.hpp"
#include "tenzir/index.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <string_view>

namespace tenzir {

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
  namespace sd = tenzir::defaults;
  tenzir::index_config index_config;
  const auto* settings = get_if(&args.inv.options, "tenzir.index");
  if (settings) {
    tenzir::data as_data{};
    if (!convert(*settings, as_data))
      return caf::make_error(ec::convert_error, fmt::format("failed to convert "
                                                            "{} to data",
                                                            *settings));
    if (auto err = convert(as_data, index_config))
      return err;
    TENZIR_VERBOSE("using customized indexing configuration {}", index_config);
  }
  auto handle = self->spawn<caf::detached>(
    index, accountant, filesystem, catalog, indexdir,
    // TODO: Pass these options as a tenzir::data object instead.
    std::string{sd::store_backend},
    opt("tenzir.max-partition-size", sd::max_partition_size),
    opt("tenzir.active-partition-timeout", sd::active_partition_timeout),
    opt("tenzir.max-resident-partitions", sd::max_in_mem_partitions),
    opt("tenzir.max-taste-partitions", sd::taste_partitions),
    opt("tenzir.max-queries", sd::num_query_supervisors),
    std::filesystem::path{opt("tenzir.catalog-dir", indexdir.string())},
    std::move(index_config));
  TENZIR_VERBOSE("{} spawned the index", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
