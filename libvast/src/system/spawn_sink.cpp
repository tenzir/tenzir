//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_sink.hpp"

#include "vast/config.hpp"
#include "vast/error.hpp"
#include "vast/format/writer.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_arguments.hpp"

#if VAST_ENABLE_PCAP
#  include "vast/format/pcap.hpp"
#endif // VAST_ENABLE_PCAP

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

#include <string>

using namespace std::string_literals;

namespace vast::system {

namespace {

caf::expected<caf::actor>
spawn_generic_sink(caf::local_actor* self, spawn_arguments& args,
                   std::string output_format) {
  // Bail out early for bogus invocations.
  if (caf::get_or(args.inv.options, "vast.node", false))
    return caf::make_error(ec::parse_error, "cannot start a local node");
  if (!args.empty())
    return unexpected_arguments(args);
  auto writer = format::writer::make(output_format, args.inv.options);
  if (!writer)
    return writer.error();
  return self->spawn(sink, std::move(*writer), 0u);
}

} // namespace <anonymous>

caf::expected<caf::actor>
spawn_pcap_sink([[maybe_unused]] caf::local_actor* self,
                [[maybe_unused]] spawn_arguments& args) {
#if !VAST_ENABLE_PCAP
  return caf::make_error(ec::unspecified, "not compiled with pcap support");
#else // VAST_ENABLE_PCAP
  // Bail out early for bogus invocations.
  if (caf::get_or(args.inv.options, "vast.node", false))
    return caf::make_error(ec::parse_error, "cannot start a local node");
  if (!args.empty())
    return unexpected_arguments(args);
  auto writer = std::make_unique<format::pcap::writer>(args.inv.options);
  return self->spawn(sink, std::move(writer), 0u);
#endif // VAST_ENABLE_PCAP
}

caf::expected<caf::actor>
spawn_ascii_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink(self, args, "ascii"s);
}

caf::expected<caf::actor>
spawn_csv_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink(self, args, "csv"s);
}

caf::expected<caf::actor>
spawn_json_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink(self, args, "json"s);
}

caf::expected<caf::actor>
spawn_zeek_sink(caf::local_actor* self, spawn_arguments& args) {
  return spawn_generic_sink(self, args, "zeek"s);
}

} // namespace vast::system
