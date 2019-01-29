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

#include <caf/config_value.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>
#include <caf/settings.hpp>
#include <caf/string_algorithms.hpp>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/schema.hpp"

namespace vast::system {

caf::expected<expression> normalized_and_valided(const spawn_arguments& args) {
  if (args.empty())
    return make_error(ec::syntax_error, "no query expression given");
  if (auto e = to<expression>(caf::join(args.first, args.last, " ")); !e)
    return std::move(e.error());
  else
    return normalize_and_validate(*e);
}

caf::expected<caf::optional<schema>> read_schema(const spawn_arguments& args) {
  auto schema_file_ptr = caf::get_if<std::string>(&args.options, "schema");
  if (!schema_file_ptr)
    return caf::optional<schema>{caf::none};
  VAST_UNBOX_VAR(str, load_contents(*schema_file_ptr));
  VAST_UNBOX_VAR(result, to<schema>(str));
  return caf::optional<schema>{std::move(result)};
}

caf::error unexpected_arguments(const spawn_arguments& args) {
  return make_error(ec::syntax_error, "unexpected argument(s)",
                    caf::join(args.first, args.last, " "));
}

} // namespace vast::system
