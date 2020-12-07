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

#include "vast/fwd.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/flush_listener_actor.hpp"
#include "vast/system/query_supervisor.hpp"

#include <caf/config_value.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <memory>

namespace vast::system {

// clang-format off
using index_actor = caf::typed_actor<
  caf::reacts_to<atom::worker, query_supervisor_actor>,
  caf::reacts_to<atom::done, uuid>,
  caf::replies_to<caf::stream<table_slice>>
    ::with<caf::inbound_stream_slot<table_slice>>,
  caf::reacts_to<accountant_type>,
  caf::replies_to<atom::status, status_verbosity>
    ::with<caf::config_value::dictionary>,
  caf::reacts_to<atom::subscribe, atom::flush, flush_listener_actor>,
  caf::reacts_to<expression>,
  caf::reacts_to<uuid, uint32_t>,
  caf::reacts_to<atom::replace, uuid, std::shared_ptr<partition_synopsis>>,
  caf::replies_to<atom::erase, uuid>
    ::with<ids>
>;
// clang-format on

} // namespace vast::system
