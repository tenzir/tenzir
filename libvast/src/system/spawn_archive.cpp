//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_archive.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/table_slice.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

using namespace vast::binary_byte_literals;

namespace vast::system {

caf::expected<caf::actor>
spawn_archive(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  namespace sd = vast::defaults::system;
  if (!args.empty())
    return unexpected_arguments(args);
  auto segments = get_or(args.inv.options, "vast.segments", sd::segments);
  auto max_segment_size
    = 1_MiB
      * get_or(args.inv.options, "vast.max-segment-size", sd::max_segment_size);
  auto handle
    = self->spawn(archive, args.dir / args.label, segments, max_segment_size);
  VAST_VERBOSE("{} spawned the archive", self);
  if (auto [accountant] = self->state.registry.find<accountant_actor>();
      accountant)
    self->send(handle, accountant);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
