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

#ifndef VAST_SYSTEM_RUN_READER_HPP
#define VAST_SYSTEM_RUN_READER_HPP

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
      : reader_command_base(parent, name),
        input_("-"),
        uds_(false) {
    this->add_opt("read,r", "path to input where to read events from", input_);
    this->add_opt("schema,s", "path to alternate schema", schema_file_);
    this->add_opt("uds,d", "treat -r as listening UNIX domain socket", uds_);
  }

protected:
  expected<caf::actor> make_source(caf::scoped_actor& self,
                                   caf::message args) override {
    CAF_LOG_TRACE(CAF_ARG(args));
    auto in = detail::make_input_stream(input_, uds_);
    if (!in)
      return in.error();
    Reader reader{std::move(*in)};
    auto src = self->spawn(source<Reader>, std::move(reader));
    // Supply an alternate schema, if requested.
    if (!schema_file_.empty()) {
      auto str = load_contents(schema_file_);
      if (!str)
        return str.error();
      auto sch = to<schema>(*str);
      if (!sch)
        return sch.error();
      // Send anonymously, since we can't process the reply here.
      anon_send(src, put_atom::value, std::move(*sch));
    }
    // Attempt to parse the remainder as an expression.
    if (!args.empty()) {
      auto str = args.get_as<std::string>(0);
      for (size_t i = 1; i < args.size(); ++i)
        str += ' ' + args.get_as<std::string>(i);
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

private:
  std::string input_;
  std::string schema_file_;
  bool uds_;
};

} // namespace vast::system

#endif
