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

#include <string>

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/node.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_arguments.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif // VAST_HAVE_PCAP

namespace vast::system {

namespace {

template <class Writer, class Defaults>
maybe_actor spawn_generic_sink(caf::local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  std::string category = Defaults::category;
  VAST_UNBOX_VAR(out, detail::make_output_stream<Defaults>(args.options));
  return self->spawn(sink<Writer>, Writer{std::move(out)}, 0u);
}

} // namespace <anonymous>

maybe_actor spawn_pcap_sink([[maybe_unused]] caf::local_actor* self,
                            [[maybe_unused]] spawn_arguments& args) {
  using defaults_t = defaults::export_::pcap;
  std::string category = defaults_t::category;
#ifndef VAST_HAVE_PCAP
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  if (!args.empty())
    return unexpected_arguments(args);
  format::pcap::writer writer{caf::get_or(args.options, category + ".write",
                                          defaults_t::write),
                              caf::get_or(args.options,
                                          category + ".flush-interval",
                                          defaults_t::flush_interval)};
  return self->spawn(sink<format::pcap::writer>, std::move(writer), 0u);
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_zeek_sink(caf::local_actor* self, spawn_arguments& args) {
  using defaults_t = defaults::export_::zeek;
  std::string category = defaults_t::category;
  if (!args.empty())
    return unexpected_arguments(args);
  format::zeek::writer writer{
    get_or(args.options, category + ".write", defaults_t::write)};
  return self->spawn(sink<format::zeek::writer>, std::move(writer), 0u);
}

maybe_actor spawn_ascii_sink(caf::local_actor* self, spawn_arguments& args) {
  auto f = spawn_generic_sink<format::ascii::writer, defaults::export_::ascii>;
  return f(self, args);
}

maybe_actor spawn_csv_sink(caf::local_actor* self, spawn_arguments& args) {
  auto f = spawn_generic_sink<format::csv::writer, defaults::export_::csv>;
  return f(self, args);
}

maybe_actor spawn_json_sink(caf::local_actor* self, spawn_arguments& args) {
  auto f = spawn_generic_sink<format::json::writer, defaults::export_::json>;
  return f(self, args);
}

} // namespace vast::system
