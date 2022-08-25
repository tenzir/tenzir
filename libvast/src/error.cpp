//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/error.hpp"

#include "vast/detail/assert.hpp"

#include <caf/pec.hpp>
#include <caf/sec.hpp>

#include <sstream>
#include <string>

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
  "timeout",
  "stalled",
  "incomplete",
  "version_error",
  "syntax_error",
  "lookup_error",
  "logic_error",
  "invalid_table_slice_type",
  "invalid_synopsis_type",
  "remote_node_down",
  "invalid_argument",
  "invalid_result",
  "invalid_configuration",
  "unrecognized_option",
  "invalid_subcommand",
  "missing_subcommand",
  "missing_component",
  "unimplemented",
  "recursion_limit_reached",
  "silent",
  "out_of_memory",
  "system_error",
  "breaking_change",
  "serialization_error",
};

static_assert(ec{std::size(descriptions)} == ec::ec_count,
              "Mismatch between number of error codes and descriptions");

void render_default_ctx(std::ostringstream& oss, const caf::message& ctx) {
  size_t size = ctx.size();
  if (size > 0) {
    oss << ":";
    for (size_t i = 0; i < size; ++i) {
      oss << ' ';
      if (ctx.match_element<std::string>(i))
        oss << ctx.get_as<std::string>(i);
      else
        oss << to_string(ctx);
    }
  }
}

} // namespace

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  VAST_ASSERT(index < sizeof(descriptions));
  return descriptions[index];
}

std::string render(caf::error err) {
  if (!err)
    return "";
  std::ostringstream oss;
  auto category = err.category();
  if (category
        == caf::type_id_v<
          vast::ec> && static_cast<vast::ec>(err.code()) == ec::silent)
    return "";
  oss << "!! ";
  switch (category) {
    default:
      oss << "Unknown";
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<vast::ec>:
      oss << to_string(static_cast<vast::ec>(err.code()));
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<caf::pec>:
      oss << to_string(static_cast<caf::pec>(err.code()));
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<caf::sec>:
      oss << to_string(static_cast<caf::sec>(err.code()));
      render_default_ctx(oss, err.context());
      break;
  }
  return oss.str();
}

} // namespace vast
