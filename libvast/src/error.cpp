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

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

namespace vast {
namespace {

const char* descriptions[] = {
  "no_error",
  "unspecified",
  "filesystem_error",
  "type_clash",
  "unsupported_operator",
  "parse_error",
  "print_error",
  "convert_error",
  "invalid_query",
  "format_error",
  "end_of_input",
  "version_error",
  "syntax_error",
  "invalid_table_slice_type",
  "invalid_synopsis_type",
  "remote_node_down",
  "invalid_result",
  "invalid_configuration",
  "unrecognized_option",
  "invalid_subcommand",
  "missing_subcommand",
  "no_importer",
  "unimplemented",
};

} // namespace <anonymous>

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  VAST_ASSERT(index < sizeof(descriptions));
  return descriptions[index];
}

} // namespace vast
