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

#include <caf/all.hpp>

#include <caf/detail/scope_guard.hpp>

#include "vast/config.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn.hpp"
#include "vast/system/format_factory.hpp"

#include "vast/detail/make_io_stream.hpp"

using namespace std::string_literals;
using namespace caf;

namespace vast {
namespace system {

expected<actor> spawn_source(caf::stateful_actor<node_state>* self,
                             options& opts) {
  if (opts.params.empty())
    return make_error(ec::syntax_error, "missing format");
  auto& format = opts.params.get_as<std::string>(0);
  auto source_args = opts.params.drop(1);
  std::string schema_file;
  auto r = source_args.extract_opts(
    {{"schema,s", "path to alternate schema", schema_file}});
  actor src;
  if (auto reader = self->state.formats.reader(format); reader)
    if (auto source_actor = (*reader)(self, r.remainder); source_actor)
      src = *source_actor;
    else
      return source_actor.error();
  else
    return reader.error();
  // Supply an alternate schema, if requested.
  if (!schema_file.empty()) {
    auto str = load_contents(schema_file);
    if (!str)
      return str.error();
    auto sch = to<schema>(*str);
    if (!sch)
      return sch.error();
    // Send anonymously, since we can't process the reply here.
    anon_send(src, put_atom::value, std::move(*sch));
  }
  // Attempt to parse the remainder as an expression.
  if (!r.remainder.empty()) {
    auto str = r.remainder.get_as<std::string>(0);
    for (auto i = 1u; i < r.remainder.size(); ++i)
      str += ' ' + r.remainder.get_as<std::string>(i);
    auto expr = to<expression>(str);
    if (!expr)
      return expr.error();
    expr = normalize_and_validate(*expr);
    if (!expr)
      return expr.error();
    r.remainder = {};
    anon_send(src, std::move(*expr));
  }
  return src;
}

} // namespace system
} // namespace vast
