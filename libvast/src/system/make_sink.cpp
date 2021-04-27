//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/make_sink.hpp"

#include "vast/defaults.hpp"
#include "vast/format/writer.hpp"
#include "vast/system/make_transforms.hpp"
#include "vast/system/sink.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <string>

namespace vast::system {

caf::expected<caf::actor>
make_sink(caf::actor_system& sys, const std::string& output_format,
          const caf::settings& options) {
  auto writer = format::writer::make(output_format, options);
  if (!writer)
    return writer.error();
  auto max_events
    = get_or(options, "vast.export.max-events", defaults::export_::max_events);
  auto transforms = make_transforms(transforms_location::client_sink, options);
  if (!transforms)
    return transforms.error();
  return sys.spawn(transforming_sink, std::move(*writer),
                   std::move(*transforms), max_events);
}

} // namespace vast::system
