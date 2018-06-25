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

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/logger.hpp"

#include "vast/system/writer_command_base.hpp"
#include "vast/system/sink.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

/// Default implementation for export sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Writer>
class writer_command : public writer_command_base {
public:
  writer_command(command* parent, std::string_view name)
      : writer_command_base{parent, name} {
    add_opt<std::string>("write,w", "path to write events to");
    add_opt<bool>("uds,d", "treat -w as UNIX domain socket to connect to");
  }

protected:
  expected<caf::actor> make_sink(caf::scoped_actor& self,
                                 const caf::config_value_map& options,
                                 argument_iterator begin,
                                 argument_iterator end) override {
    VAST_UNUSED(begin, end);
    VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
    using ostream_ptr = std::unique_ptr<std::ostream>;
    auto limit = get_or(options, "events", defaults::command::max_events);
    VAST_ASSERT(limit);
    if constexpr (std::is_constructible_v<Writer, ostream_ptr>) {
      auto output = get_or(options, "write", defaults::command::write_path);
      auto uds = get_or(options, "uds", false);
      auto out = detail::make_output_stream(output, uds);
      if (!out)
        return out.error();
      Writer writer{std::move(*out)};
      return self->spawn(sink<Writer>, std::move(writer), limit);
    } else {
      Writer writer;
      return self->spawn(sink<Writer>, std::move(writer), limit);
    }
  }
};

} // namespace vast::system

