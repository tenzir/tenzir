//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/add_message_types.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/bitmap.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/command.hpp"
#include "tenzir/component_registry.hpp"
#include "tenzir/config.hpp"
#include "tenzir/connect_request.hpp"
#include "tenzir/context.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/die.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/http_api.hpp"
#include "tenzir/index.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/module.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/package.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pattern.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/port.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/query_cursor.hpp"
#include "tenzir/query_options.hpp"
#include "tenzir/query_status.hpp"
#include "tenzir/resource.hpp"
#include "tenzir/series.hpp"
#include "tenzir/shared_diagnostic_handler.hpp"
#include "tenzir/status.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <caf/init_global_meta_objects.hpp>
#include <caf/inspector_access.hpp>
#include <caf/io/middleman.hpp>
#include <caf/stream.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <utility>
#include <vector>

namespace tenzir::detail {

void add_message_types() {
  caf::core::init_global_meta_objects();
  caf::io::middleman::init_global_meta_objects();
  caf::init_global_meta_objects<caf::id_block::tenzir_types>();
  caf::init_global_meta_objects<caf::id_block::tenzir_atoms>();
  caf::init_global_meta_objects<caf::id_block::tenzir_actors>();
  auto old_blocks = std::vector<plugin_type_id_block>{
    {caf::id_block::tenzir_types::begin, caf::id_block::tenzir_actors::end}};
  // Check for type ID conflicts between dynamic plugins.
  for (const auto& [new_block, assigner] :
       plugins::get_static_type_id_blocks()) {
    for (const auto& old_block : old_blocks) {
      if (new_block.begin < old_block.end && old_block.begin < new_block.end) {
        die("cannot assign overlapping plugin type ID blocks");
      }
    }
    old_blocks.push_back(new_block);
    assigner();
  }
}

} // namespace tenzir::detail
