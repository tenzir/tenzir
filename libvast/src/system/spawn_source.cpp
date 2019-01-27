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

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/test.hpp"
#include "vast/system/node.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn_arguments.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/format/pcap.hpp"
#endif // VAST_HAVE_PCAP

namespace vast::system {

namespace {

template <class Reader, class... Ts>
maybe_actor spawn_generic_source(caf::local_actor* self, spawn_arguments& args,
                                 Ts&&... ctor_args) {
  VAST_UNBOX_VAR(expr, normalized_and_valided(args));
  VAST_UNBOX_VAR(sch, read_schema(args));
  VAST_UNBOX_VAR(out, detail::make_output_stream(args.options));
  auto global_table_slice_type = get_or(self->system().config(),
                                        "vast.table-slice-type",
                                        defaults::system::table_slice_type);
  auto table_slice_type = get_or(args.options, "table-slice",
                                 global_table_slice_type);
  Reader reader{table_slice_type, std::forward<Ts>(ctor_args)...};
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
  VAST_UNUSED(self, args);
  return make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_HAVE_PCAP
  auto opt = [&](caf::string_view key, auto default_value) {
    return get_or(args.options, key, default_value);
  };
  namespace cd = defaults::command;
  return spawn_generic_source<format::pcap::reader>(
    self, args, opt("global.read", cd::read_path),
    opt("global.cutoff", cd::cutoff), opt("global.flow-max", cd::max_flows),
    opt("global.flow-age", cd::max_flow_age),
    opt("global.flow-expiry", cd::flow_expiry),
    opt("global.pseudo-realtime", cd::pseudo_realtime_factor));
#endif // VAST_HAVE_PCAP
}

maybe_actor spawn_test_source(caf::local_actor* self, spawn_arguments& args) {
  using reader_type = format::test::reader;
  VAST_UNBOX_VAR(sch, read_schema(args));
  // The test source only generates events out of thin air and thus accepts no
  // source expression.
  if (!args.empty())
    return unexpected_arguments(args);
  auto global_table_slice_type = get_or(self->system().config(),
                                        "vast.table-slice-type",
                                        defaults::system::table_slice_type);
  auto table_slice_type = get_or(args.options, "table-slice",
                                 global_table_slice_type);
  reader_type reader{table_slice_type,
                     get_or(args.options, "global.seed", size_t{0}),
                     get_or(args.options, "global.events", size_t{100})};
  auto src = self->spawn(default_source<reader_type>, std::move(reader));
  if (sch)
    caf::anon_send(src, put_atom::value, std::move(*sch));
  return src;
}

maybe_actor spawn_bro_source(caf::local_actor* self, spawn_arguments& args) {
  VAST_UNBOX_VAR(in, detail::make_input_stream(args.options));
  return spawn_generic_source<format::bro::reader>(self, args, std::move(in));
}

maybe_actor spawn_bgpdump_source(caf::local_actor* self,
                                 spawn_arguments& args) {
  VAST_UNBOX_VAR(in, detail::make_input_stream(args.options));
  return spawn_generic_source<format::bgpdump::reader>(self, args,
                                                       std::move(in));
}

maybe_actor spawn_mrt_source(caf::local_actor* self, spawn_arguments& args) {
  VAST_UNBOX_VAR(in, detail::make_input_stream(args.options));
  return spawn_generic_source<format::mrt::reader>(self, args, std::move(in));
}

} // namespace vast::system
