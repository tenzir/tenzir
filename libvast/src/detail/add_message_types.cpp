//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/add_message_types.hpp"

#include "vast/address.hpp"
#include "vast/atoms.hpp"
#include "vast/bitmap.hpp"
#include "vast/chunk.hpp"
#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/detail/stable_map.hpp"
#include "vast/die.hpp"
#include "vast/expression.hpp"
#include "vast/operator.hpp"
#include "vast/pattern.hpp"
#include "vast/plugin.hpp"
#include "vast/port.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/query.hpp"
#include "vast/query_options.hpp"
#include "vast/schema.hpp"
#include "vast/subnet.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/component_registry.hpp"
#include "vast/system/query_status.hpp"
#include "vast/system/report.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/taxonomies.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/stream.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <utility>
#include <vector>

namespace vast::detail {

void add_message_types(caf::actor_system_config& cfg) {
  cfg.add_message_types<caf::id_block::vast_types>();
  cfg.add_message_types<caf::id_block::vast_atoms>();
  cfg.add_message_types<caf::id_block::vast_actors>();
  for (const auto& [new_block, assigner] :
       plugins::get_static_type_id_blocks()) {
    // Check for type ID conflicts between static plugins.
    static auto old_blocks = std::vector<plugin_type_id_block>{
      {caf::id_block::vast_types::begin, caf::id_block::vast_actors::end}};
    for (const auto& old_block : old_blocks)
      if (new_block.begin < old_block.end && old_block.begin < new_block.end)
        die("cannot assign overlapping plugin type ID blocks");
    old_blocks.push_back(new_block);
    assigner(cfg);
  }
}

} // namespace vast::detail
