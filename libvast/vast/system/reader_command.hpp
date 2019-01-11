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
#include <string_view>
#include <utility>

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>

#include "vast/command.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/source.hpp"
#include "vast/system/source_command.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Reader>
caf::message reader_command(const command& cmd, caf::actor_system& sys,
                            caf::config_value_map& options,
                            command::argument_iterator first,
                            command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  auto input = get_or(options, "read", defaults::command::read_path);
  auto uds = get_or(options, "uds", false);
  auto table_slice = get_or(options, "table-slice",
                            defaults::system::table_slice_type);
  auto table_slice_id = caf::atom_from_string(table_slice);
  auto factory = get_table_slice_builder_factory(table_slice_id);
  if (factory == nullptr)
    return caf::make_message(make_error(ec::unspecified,
                                        "unknown table_slice_builder factory"));
  auto in = detail::make_input_stream(input, uds);
  if (!in)
    return caf::make_message(std::move(in.error()));
  Reader reader{std::move(*in)};
  auto slice_size = get_or(options, "table-slice-size",
                           defaults::system::table_slice_size);
  auto src = sys.spawn(source<Reader>, std::move(reader), factory, slice_size);
  return source_command(cmd, sys, std::move(src), options, first, last);
}

} // namespace vast::system
