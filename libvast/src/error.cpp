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

#include <sstream>

#include <caf/pec.hpp>
#include <caf/sec.hpp>

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

namespace vast {
namespace {

const char* descriptions[] = {
  "no_error",
  "unspecified",
  "no_such_file",
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

void render_default_ctx(std::ostringstream& oss, const caf::message& ctx) {
  size_t size = ctx.size();
  if (size > 0) {
    oss << ":";
    for (size_t i = 0; i < size; ++i)
      oss << " \"" << ctx.get_as<std::string>(i) << '"';
  }
}

} // namespace

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  VAST_ASSERT(index < sizeof(descriptions));
  return descriptions[index];
}

std::string render(caf::error err) {
  using caf::atom_uint;
  std::ostringstream oss;
  auto category = err.category();
  oss << "!! ";
  switch (atom_uint(category)) {
    default:
      oss << "unknown error category: " << to_string(category);
      render_default_ctx(oss, err.context());
    case atom_uint("vast"): {
      auto x = static_cast<vast::ec>(err.code());
      oss << to_string(x);
      switch (x) {
        default:
          render_default_ctx(oss, err.context());
      }
    }
    case atom_uint("parser"):
      oss << to_string(static_cast<caf::pec>(err.code()));
      render_default_ctx(oss, err.context());
    case atom_uint("system"):
      oss << to_string(static_cast<caf::sec>(err.code()));
      render_default_ctx(oss, err.context());
  }
  oss << '\n';
  return oss.str();
}

} // namespace vast
