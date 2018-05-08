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
    add_opt("write,w", "path to write events to", "-");
    add_opt("uds,d", "treat -w as UNIX domain socket to connect to", false);
  }

protected:
  expected<caf::actor> make_sink(caf::scoped_actor& self,
                                 const option_map& options,
                                 argument_iterator begin,
                                 argument_iterator end) override {
    VAST_UNUSED(begin, end);
    VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
    using ostream_ptr = std::unique_ptr<std::ostream>;
    auto limit = get<uint64_t>(options, "events");
    VAST_ASSERT(limit);
    if constexpr (std::is_constructible_v<Writer, ostream_ptr>) {
      auto output = get<std::string>(options, "write");
      VAST_ASSERT(output);
      auto uds = get<bool>(options, "uds");
      VAST_ASSERT(uds);
      auto out = detail::make_output_stream(*output, *uds);
      if (!out)
        return out.error();
      Writer writer{std::move(*out)};
      return self->spawn(sink<Writer>, std::move(writer), *limit);
    } else {
      Writer writer;
      return self->spawn(sink<Writer>, std::move(writer), *limit);
    }
  }
};

} // namespace vast::system

