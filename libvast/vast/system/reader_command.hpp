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

#include <string>
#include <string_view>
#include <utility>

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/logger.hpp"
#include "vast/system/source.hpp"
#include "vast/system/source_command.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Reader>
class reader_command : public source_command {
public:
  reader_command(command* parent) : source_command(parent) {
    add_opt<std::string>("read,r", "path to input where to read events from");
    add_opt<bool>("uds,d", "treat -r as listening UNIX domain socket");
  }

protected:
  expected<caf::actor>
  make_source(caf::scoped_actor& self,
              const caf::config_value_map& options) override {
    VAST_TRACE("");
    auto input = get_or(options, "read", defaults::command::read_path);
    auto uds = get_or(options, "uds", false);
    auto in = detail::make_input_stream(input, uds);
    if (!in)
      return in.error();
    Reader reader{std::move(*in)};
    return self->spawn(default_source<Reader>, std::move(reader));
  }
};

} // namespace vast::system
