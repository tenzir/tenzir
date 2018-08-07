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

#pragma once

#include "vast/logger.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/default_table_slice.hpp"
#include "vast/defaults.hpp"

namespace vast::system {

struct broker_source_state {
  // TODO: factor this type from vast::source_state<Reader>.
  using factory_type = table_slice_builder_ptr (*)(record_type);
};

/// A Broker event producer.
/// @param self The actor handle.
caf::behavior broker_source(caf::stateful_actor<broker_source_state>* self,
                            broker_source_state::factory_type factory,
                            size_t tble_slice_size) {
  return {};
}

/// An event producer with default table slice settings.
caf::behavior default_broker_source(caf::stateful_actor<broker_source_state>* self) {
  auto slice_size = get_or(self->system().config(), "system.table-slice-size",
                           defaults::system::table_slice_size);
  return broker_source(self, default_table_slice::make_builder, slice_size);
}

} // namespace vast::system
