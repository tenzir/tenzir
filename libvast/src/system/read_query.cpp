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

#include "vast/system/read_query.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/tracker.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>

using namespace caf;
using namespace std::chrono_literals;

namespace vast::system {

caf::expected<std::string> read_query(const command::invocation& invocation,
                                      std::string_view option_name) {
  VAST_TRACE(invocation, option_name);
  std::string result;
  auto assign_query = [&](std::istream& in) {
    result.assign(std::istreambuf_iterator<char>{in},
                  std::istreambuf_iterator<char>{});
  };
  if (auto fname = caf::get_if<std::string>(&invocation.options, option_name)) {
    // Sanity check.
    if (!invocation.arguments.empty())
      return make_error(ec::parse_error, "got a query on the command line "
                                         "but --read option is defined");
    // Read query from STDIN if file name is '-'.
    if (*fname == "-")
      assign_query(std::cin);
    else {
      std::ifstream f{*fname};
      if (!f)
        return make_error(ec::no_such_file, "unable to read from " + *fname);
      assign_query(f);
    }
  } else if (invocation.arguments.empty()) {
    // Read query from STDIN.
    assign_query(std::cin);
  } else {
    // Assemble expression from all remaining arguments.
    result = detail::join(invocation.arguments.begin(),
                          invocation.arguments.end(), " ");
  }
  if (result.empty())
    return make_error(ec::invalid_query);
  return result;
}

} // namespace vast::system
