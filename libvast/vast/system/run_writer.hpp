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

#ifndef VAST_SYSTEM_RUN_WRITER_HPP
#define VAST_SYSTEM_RUN_WRITER_HPP

#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/logger.hpp"

#include "vast/system/run_writer_base.hpp"
#include "vast/system/sink.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

/// Default implementation for export sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Writer>
class run_writer : public run_writer_base {
public:
  run_writer(command* parent, std::string_view name)
      : run_writer_base(parent, name),
        output("-"),
        uds(false) {
    this->add_opt("write,w", "path to write events to", output);
    this->add_opt("uds,d", "treat -w as UNIX domain socket to connect to", uds);
  }

protected:
  expected<caf::actor> make_sink(caf::scoped_actor& self,
                                 caf::message args) override {
    CAF_LOG_TRACE(CAF_ARG(args));
    using ostream_ptr = std::unique_ptr<std::ostream>;
    if constexpr (std::is_constructible<Writer, ostream_ptr>::value) {
      auto out = detail::make_output_stream(output, uds);
      if (!out)
        return out.error();
      Writer writer{std::move(*out)};
      return self->spawn(sink<Writer>, std::move(writer));
    } else {
      Writer writer;
      return self->spawn(sink<Writer>, std::move(writer));
    }
  }

private:
  std::string output;
  bool uds;
};

} // namespace vast::system

#endif
