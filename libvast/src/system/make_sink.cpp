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

#include "vast/system/make_sink.hpp"

#include "vast/defaults.hpp"
#include "vast/format/writer_factory.hpp"
#include "vast/system/sink.hpp"

#include <caf/actor.hpp>
#include <caf/actor_system.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <string>

namespace vast::system {

caf::expected<caf::actor>
make_sink(caf::actor_system& sys, const std::string& output_format,
          const caf::settings& options) {
  auto writer = factory<format::writer>::make(output_format, options);
  if (!writer)
    return writer.error();
  auto max_events
    = get_or(options, "vast.export.max-events", defaults::export_::max_events);
  return sys.spawn(sink, std::move(*writer), max_events);
}

} // namespace vast::system
