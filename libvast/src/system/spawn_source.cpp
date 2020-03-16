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

#include "vast/system/spawn_source.hpp"

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/syslog.hpp"
#include "vast/format/test.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/node.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>

#include <string>

#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif // VAST_HAVE_PCAP

namespace vast::system {

namespace {

template <class Reader, class... Ts>
maybe_actor spawn_generic_source(caf::local_actor* self, spawn_arguments& args,
                                 Ts&&... ctor_args) {
  VAST_UNBOX_VAR(expr, normalized_and_validated(args));
  VAST_UNBOX_VAR(sch, read_schema(args));
  auto table_slice_type = defaults::import::table_slice_type(
    self->system(), args.invocation.options);
  Reader reader{table_slice_type, args.invocation.options,
                std::forward<Ts>(ctor_args)...};
  auto src = self->spawn(default_source<Reader>, std::move(reader));
  caf::anon_send(src, std::move(expr));
  if (sch)
    caf::anon_send(src, put_atom::value, std::move(*sch));
  return src;
}

} // namespace <anonymous>

maybe_actor spawn_pcap_source([[maybe_unused]] caf::local_actor* self,
                              [[maybe_unused]] spawn_arguments& args) {
#ifndef VAST_HAVE_PCAP
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  using defaults_t = defaults::import::pcap;
  VAST_UNBOX_VAR(
    in, detail::make_input_stream<defaults_t>(args.invocation.options));
  return spawn_generic_source<format::pcap::reader>(self, args, std::move(in));
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_syslog_source([[maybe_unused]] caf::local_actor* self,
                                [[maybe_unused]] spawn_arguments& args) {
  using defaults_t = defaults::import::syslog;
  VAST_UNBOX_VAR(
    in, detail::make_input_stream<defaults_t>(args.invocation.options));
  return spawn_generic_source<format::syslog::reader>(self, args,
                                                      std::move(in));
}

maybe_actor spawn_test_source(caf::local_actor* self, spawn_arguments& args) {
  using reader_type = format::test::reader;
  VAST_UNBOX_VAR(sch, read_schema(args));
  // The test source only generates events out of thin air and thus accepts no
  // source expression.
  if (!args.empty())
    return unexpected_arguments(args);
  auto table_slice_type = defaults::import::table_slice_type(
    self->system(), args.invocation.options);
  using defaults_t = defaults::import::test;
  std::string category = defaults_t::category;
  reader_type reader{table_slice_type,
                     defaults_t::seed(args.invocation.options),
                     get_or(args.invocation.options, "import.max-events",
                            defaults::import::max_events)};
  auto src = self->spawn(default_source<reader_type>, std::move(reader));
  if (sch)
    caf::anon_send(src, put_atom::value, std::move(*sch));
  return src;
}

maybe_actor spawn_zeek_source(caf::local_actor* self, spawn_arguments& args) {
  using defaults_t = defaults::import::zeek;
  VAST_UNBOX_VAR(
    in, detail::make_input_stream<defaults_t>(args.invocation.options));
  return spawn_generic_source<format::zeek::reader>(self, args, std::move(in));
}

maybe_actor spawn_bgpdump_source(caf::local_actor* self,
                                 spawn_arguments& args) {
  using defaults_t = defaults::import::bgpdump;
  VAST_UNBOX_VAR(
    in, detail::make_input_stream<defaults_t>(args.invocation.options));
  return spawn_generic_source<format::bgpdump::reader>(self, args,
                                                       std::move(in));
}

maybe_actor spawn_mrt_source(caf::local_actor* self, spawn_arguments& args) {
  using defaults_t = defaults::import::mrt;
  VAST_UNBOX_VAR(
    in, detail::make_input_stream<defaults_t>(args.invocation.options));
  return spawn_generic_source<format::mrt::reader>(self, args, std::move(in));
}

} // namespace vast::system
