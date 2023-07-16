//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/make_sink.hpp"

#include "tenzir/defaults.hpp"
#include "tenzir/format/writer.hpp"
#include "tenzir/sink.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <string>

namespace tenzir {

caf::expected<caf::actor>
make_sink(caf::actor_system& sys, const std::string& output_format,
          const caf::settings& options) {
  auto writer = format::writer::make(output_format, options);
  if (!writer)
    return writer.error();
  auto max_events = get_or(options, "tenzir.export.max-events",
                           defaults::export_::max_events);
  return sys.spawn(sink, std::move(*writer), max_events);
}

} // namespace tenzir
