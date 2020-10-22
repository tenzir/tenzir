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

#include "vast/system/spawn_arguments.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"

#include <caf/config_value.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>
#include <caf/settings.hpp>
#include <caf/string_algorithms.hpp>

namespace vast::system {

caf::expected<expression>
normalized_and_validated(std::vector<std::string>::const_iterator begin,
                         std::vector<std::string>::const_iterator end) {
  if (begin == end)
    return make_error(ec::syntax_error, "no query expression given");
  if (auto e = to<expression>(caf::join(begin, end, " ")))
    return normalize_and_validate(*e);
  else
    return std::move(e.error());
}

caf::expected<expression>
normalized_and_validated(const std::vector<std::string>& args) {
  return normalized_and_validated(args.begin(), args.end());
}

caf::expected<expression>
normalized_and_validated(const spawn_arguments& args) {
  auto& arguments = args.inv.arguments;
  return normalized_and_validated(arguments.begin(), arguments.end());
}

caf::expected<expression> get_expression(const spawn_arguments& args) {
  if (args.expr)
    return *args.expr;
  auto expr = system::normalized_and_validated(args.inv.arguments);
  if (!expr)
    return expr.error();
  return *expr;
}

caf::expected<caf::optional<schema>> read_schema(const spawn_arguments& args) {
  auto schema_file_ptr = caf::get_if<std::string>(&args.inv.options, "schema");
  if (!schema_file_ptr)
    return caf::optional<schema>{caf::none};
  auto str = load_contents(*schema_file_ptr);
  if (!str)
    return str.error();
  auto result = to<schema>(*str);
  if (!result)
    return result.error();
  return caf::optional<schema>{std::move(*result)};
}

caf::error unexpected_arguments(const spawn_arguments& args) {
  return make_error(ec::syntax_error, "unexpected argument(s)",
                    caf::join(args.inv.arguments.begin(),
                              args.inv.arguments.end(), " "));
}

} // namespace vast::system
