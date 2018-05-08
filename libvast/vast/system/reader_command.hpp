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

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include "vast/system/reader_command_base.hpp"
#include "vast/system/reader_command_base.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/detail/make_io_stream.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Reader>
class reader_command : public reader_command_base {
public:
  reader_command(command* parent, std::string_view name)
      : reader_command_base(parent, name) {
    add_opt("read,r", "path to input where to read events from", "-");
    add_opt("schema,s", "path to alternate schema", "");
    add_opt("uds,d", "treat -r as listening UNIX domain socket", false);
  }

protected:
  expected<caf::actor> make_source(caf::scoped_actor& self,
                                   option_map& options,
                                   argument_iterator begin,
                                   argument_iterator end) override {
    VAST_TRACE(VAST_ARG("args", begin, end));
    auto input = get<std::string>(options, "input");
    VAST_ASSERT(input);
    auto uds = get<bool>(options, "uds");
    VAST_ASSERT(uds);
    auto in = detail::make_input_stream(*input, *uds);
    if (!in)
      return in.error();
    Reader reader{std::move(*in)};
    auto src = self->spawn(source<Reader>, std::move(reader));
    // Supply an alternate schema, if requested.
    auto schema_file = get<std::string>(options, "schema");
    VAST_ASSERT(schema_file);
    if (!schema_file->empty()) {
      auto str = load_contents(*schema_file);
      if (!str)
        return str.error();
      auto sch = to<schema>(*str);
      if (!sch)
        return sch.error();
      // Send anonymously, since we can't process the reply here.
      anon_send(src, put_atom::value, std::move(*sch));
    }
    // Attempt to parse the remainder as an expression.
    if (begin != end) {
      auto str = std::accumulate(
        std::next(begin), end, *begin,
        [](std::string a, const std::string& b) { return a += ' ' + b; });
      auto expr = to<expression>(str);
      if (!expr)
        return expr.error();
      expr = normalize_and_validate(*expr);
      if (!expr)
        return expr.error();
      anon_send(src, std::move(*expr));
    }
    return src;
  }
};

} // namespace vast::system

