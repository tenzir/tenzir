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

#include <string>
#include <type_traits>
#include <utility>

#include <caf/config_value.hpp>
#include <caf/io/middleman.hpp>

#include "vast/command.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/system/datagram_source.hpp"
#include "vast/system/source.hpp"
#include "vast/system/source_command.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Reader, class Defaults>
caf::message reader_command(const command& cmd, caf::actor_system& sys,
                            caf::settings& options,
                            command::argument_iterator first,
                            command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  std::string category = Defaults::category;
  auto uri = caf::get_if<std::string>(&options, category + ".listen");
  auto file = caf::get_if<std::string>(&options, category + ".read");
  auto max_events = caf::get_if<size_t>(&options, category + ".max-events");
  if (uri && file)
    return caf::make_message(make_error(ec::invalid_configuration,
                                        "only one source possible (-r or -l)"));
  if (!uri && !file) {
    using inputs = vast::format::reader::inputs;
    if constexpr (Reader::defaults::input == inputs::inet)
      uri = std::string{Reader::defaults::uri};
    else
      file = std::string{Reader::defaults::path};
  }
  auto slice_type = defaults::import::table_slice_type(sys, options);
  auto factory = vast::factory<vast::table_slice_builder>::get(slice_type);
  if (factory == nullptr)
    return caf::make_message(
      make_error(vast::ec::unspecified, "unknown table_slice_builder factory"));
  auto slice_size = get_or(options, "system.table-slice-size",
                           defaults::system::table_slice_size);
  if (uri) {
    endpoint ep;
    if (!vast::parsers::endpoint(*uri, ep))
      return caf::make_message(
        make_error(vast::ec::parse_error, "unable to parse endpoint", *uri));
    if (ep.port.type() == port::unknown) {
      using inputs = vast::format::reader::inputs;
      if constexpr (Reader::defaults::input == inputs::inet) {
        endpoint default_ep;
        vast::parsers::endpoint(Reader::defaults::uri, default_ep);
        ep.port = port{ep.port.number(), default_ep.port.type()};
      } else {
        // Fall back to tcp if we don't know anything else.
        ep.port = port{ep.port.number(), port::tcp};
      }
    }
    Reader reader{slice_type};
    auto run = [&](auto&& source) {
      auto& mm = sys.middleman();
      auto src = mm.spawn_broker(std::forward<decltype(source)>(source),
                                 ep.port.number(), std::move(reader), factory,
                                 slice_size, max_events);
      return source_command(cmd, sys, std::move(src), options, first, last);
    };
    switch (ep.port.type()) {
      default:
        return caf::make_message(
          make_error(vast::ec::unimplemented,
                     "port type not supported:", ep.port.type()));
      case port::udp:
        return run(datagram_source<Reader>);
    }
  } else {
    auto uds = get_or(options, category + ".uds", false);
    auto in = detail::make_input_stream(*file, uds);
    if (!in)
      return caf::make_message(std::move(in.error()));
    Reader reader{slice_type, std::move(*in)};
    auto src = sys.spawn(source<Reader>, std::move(reader), factory, slice_size,
                         max_events);
    return source_command(cmd, sys, std::move(src), options, first, last);
  }
}

} // namespace vast::system
