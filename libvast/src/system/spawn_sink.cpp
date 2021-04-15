//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_sink.hpp"

#include "vast/config.hpp"
#include "vast/error.hpp"
#include "vast/format/writer.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

#include <string>

using namespace std::string_literals;

namespace vast::system {

caf::expected<caf::actor>
spawn_sink(caf::local_actor* self, spawn_arguments& args) {
  // Bail out early for bogus invocations.
  if (caf::get_or(args.inv.options, "vast.node", false))
    return caf::make_error(ec::parse_error, "cannot start a local node");
  if (!args.empty())
    return unexpected_arguments(args);
  auto writer
    = format::writer::make(std::string{args.inv.name()}, args.inv.options);
  if (!writer)
    return writer.error();
  return self->spawn(sink, std::move(*writer), 0u);
}

} // namespace vast::system
