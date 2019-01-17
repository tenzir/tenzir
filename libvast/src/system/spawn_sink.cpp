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

#include "vast/system/spawn_sink.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/system/node.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_arguments.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif // VAST_HAVE_PCAP

namespace vast::system {

namespace {

template <class Writer>
maybe_actor spawn_generic_sink(caf::local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  VAST_UNBOX_VAR(out, detail::make_output_stream(args.options));
  return self->spawn(sink<Writer>, Writer{std::move(out)}, 0u);
}

} // namespace <anonymous>

maybe_actor spawn_pcap_sink([[maybe_unused]] caf::local_actor* self,
                            [[maybe_unused]] spawn_arguments& args) {
#ifndef VAST_HAVE_PCAP
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  if (!args.empty())
    return unexpected_arguments(args);
  format::pcap::writer writer{
    caf::get_or(args.options, "global.write", defaults::command::write_path),
    caf::get_or(args.options, "global.flush", size_t{0})};
  return self->spawn(sink<format::pcap::writer>, std::move(writer), 0u);
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_bro_sink(caf::local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  format::bro::writer writer{get_or(args.options, "global.write",
                                    defaults::command::write_path)};
  return self->spawn(sink<format::bro::writer>, std::move(writer), 0u);
}

maybe_actor spawn_ascii_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::ascii::writer>(self, args);
}

maybe_actor spawn_csv_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::csv::writer>(self, args);
}

maybe_actor spawn_json_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink<format::json::writer>(self, args);
}

} // namespace vast::system
