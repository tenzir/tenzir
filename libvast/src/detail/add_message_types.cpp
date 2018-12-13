/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include <cstdint>
#include <vector>

#include <caf/actor_system_config.hpp>

#include "vast/config.hpp"

#include "vast/bitmap.hpp"
#include "vast/event.hpp"
#include "vast/expression.hpp"
#include "vast/operator.hpp"
#include "vast/query_options.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include "vast/system/query_status.hpp"
#include "vast/system/replicated_store.hpp"
#include "vast/system/tracker.hpp"

#include "vast/detail/add_message_types.hpp"

namespace vast::detail {

void add_message_types(caf::actor_system_config& cfg) {
  cfg.add_message_type<bitmap>("vast::bitmap");
  cfg.add_message_type<data>("vast::data");
  cfg.add_message_type<event>("vast::event");
  cfg.add_message_type<expression>("vast::expression");
  cfg.add_message_type<query_options>("vast::query_options");
  cfg.add_message_type<relational_operator>("vast::relational_operator");
  cfg.add_message_type<schema>("vast::schema");
  cfg.add_message_type<type>("vast::type");
  cfg.add_message_type<uuid>("vast::uuid");
  cfg.add_message_type<table_slice_ptr>("vast::table_slice_ptr");
  // Containers
  cfg.add_message_type<std::vector<event>>("std::vector<vast::event>");
  // Actor-specific messages
  cfg.add_message_type<system::component_map>("vast::system::component_map");
  cfg.add_message_type<system::component_map_entry>(
    "vast::system::component_map_entry");
  cfg.add_message_type<system::registry>("vast::system::registry");
  cfg.add_message_type<system::query_status>(
    "vast::system::query_status");
  cfg.add_message_type<system::actor_identity>("vast::system::actor_identity");
#ifdef VAST_USE_OPENCL
  cfg.add_message_type<std::vector<uint32_t>>("std::vector<uint32_t>");
#endif
}

} // namespace vast::detail
