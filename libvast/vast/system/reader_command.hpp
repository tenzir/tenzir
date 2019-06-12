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
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
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
  auto max_events = caf::get_if<size_t>(&options, "import.max-events");
  auto slice_type = defaults::import::table_slice_type(sys, options);
  auto factory = vast::factory<vast::table_slice_builder>::get(slice_type);
  if (factory == nullptr)
    return caf::make_message(
      make_error(vast::ec::unspecified, "unknown table_slice_builder factory"));
  auto slice_size = get_or(options, "system.table-slice-size",
                           defaults::system::table_slice_size);
  // Discern the input source (file, stream, or socket).
  auto uri = caf::get_if<std::string>(&options, category + ".listen");
  auto file = caf::get_if<std::string>(&options, category + ".read");
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
  // Supply an alternate schema, if requested.
  expected<vast::schema> schema{caf::none};
  {
    auto sc = caf::get_if<std::string>(&options, category + ".schema");
    auto sf = caf::get_if<std::string>(&options, category + ".schema-file");
    if (sc && sf)
      return make_message(
        make_error(ec::invalid_configuration,
                   "had both schema and schema-file provided"));
    if (sc)
      schema = to<vast::schema>(*sc);
    if (sf)
      schema = load_schema_file(*sf);
    if (!schema && schema.error() != caf::none)
      return make_message(std::move(schema.error()));
  }
  caf::actor src;
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
    if (schema)
      reader.schema(*schema);
    auto run = [&](auto&& source) {
      auto& mm = sys.middleman();
      return mm.spawn_broker(std::forward<decltype(source)>(source),
                             ep.port.number(), std::move(reader), factory,
                             slice_size, max_events);
    };
    VAST_INFO(reader, "listens for data on", ep.host, ", port", ep.port);
    switch (ep.port.type()) {
      default:
        return caf::make_message(
          make_error(vast::ec::unimplemented,
                     "port type not supported:", ep.port.type()));
      case port::udp:
        src = run(datagram_source<Reader>);
    }
  } else {
    auto uds = get_or(options, category + ".uds", false);
    auto in = detail::make_input_stream(*file, uds);
    if (!in)
      return caf::make_message(std::move(in.error()));
    Reader reader{slice_type, std::move(*in)};
    if (schema)
      reader.schema(*schema);
    VAST_INFO(reader, "reads data from", *file);
    src = sys.spawn(source<Reader>, std::move(reader), factory, slice_size,
                    max_events);
  }
  return source_command(cmd, sys, std::move(src), options, first, last);
}

} // namespace vast::system
