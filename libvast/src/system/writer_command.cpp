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

#include "vast/system/writer_command.hpp"

#include "vast/logger.hpp"
#include "vast/system/make_sink.hpp"
#include "vast/system/sink_command.hpp"

#include <caf/actor.hpp>
#include <caf/make_message.hpp>

#include <string>

namespace vast::system {

command::fun make_writer_command(std::string_view format) {
  return [format = std::string{format}](const invocation& inv,
                                        caf::actor_system& sys) {
    VAST_TRACE(inv);
    auto snk = make_sink(sys, format, inv.options);
    if (!snk)
      return make_message(snk.error());
    return caf::make_message(sink_command(inv, sys, std::move(*snk)));
  };
}

} // namespace vast::system
