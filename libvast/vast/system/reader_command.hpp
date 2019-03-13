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
template <class Reader>
caf::message reader_command(const command& cmd, caf::actor_system& sys,
                            caf::settings& options,
                            command::argument_iterator first,
                            command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  auto uri = caf::get_if<std::string>(&options, "listen");
  auto file = caf::get_if<std::string>(&options, "read");
  if (uri && file)
    return caf::make_message(make_error(ec::invalid_configuration,
                                        "only one source possible (-r or -l)"));
  if (!uri && !file) {
    using inputs = vast::format::reader::inputs;
    if constexpr (Reader::defaults::input == inputs::inet)
      uri = Reader::defaults::uri;
    else
      file = std::string{Reader::defaults::path};
  }
  auto global_table_slice = get_or(sys.config(), "vast.table-slice-type",
                                   defaults::system::table_slice_type);
  auto table_slice = get_or(options, "table-slice", global_table_slice);
  auto factory = vast::factory<vast::table_slice_builder>::get(table_slice);
  if (factory == nullptr)
    return caf::make_message(
      make_error(vast::ec::unspecified, "unknown table_slice_builder factory"));
  if (uri) {
    endpoint ep;
    if (!vast::parsers::endpoint(*uri, ep))
      return caf::make_message(
        make_error(vast::ec::parse_error, "unable to parse endpoint", *uri));
    Reader reader{table_slice};
    auto slice_size = get_or(options, "table-slice-size",
                             defaults::system::table_slice_size);
    auto& mm = sys.middleman();
    auto src = mm.spawn_broker(datagram_source<Reader>, ep.port.number(),
                               std::move(reader), factory, slice_size);
    return source_command(cmd, sys, std::move(src), options, first, last);
  } else {
    auto uds = get_or(options, "uds", false);
    auto in = detail::make_input_stream(*file, uds);
    if (!in)
      return caf::make_message(std::move(in.error()));
    Reader reader{table_slice, std::move(*in)};
    auto slice_size = get_or(options, "table-slice-size",
                             defaults::system::table_slice_size);
    auto src = sys.spawn(source<Reader>, std::move(reader), factory,
                         slice_size);
    return source_command(cmd, sys, std::move(src), options, first, last);
  }
}

} // namespace vast::system
