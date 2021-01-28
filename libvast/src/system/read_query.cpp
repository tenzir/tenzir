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
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <unistd.h>

using namespace caf;
using namespace std::chrono_literals;

namespace vast::system {

caf::expected<std::string>
read_query(const invocation& inv, std::string_view file_option,
           size_t argument_offset) {
  VAST_TRACE("{}  {}", detail::id_or_name(inv), file_option);
  std::string result;
  auto assign_query = [&](std::istream& in) {
    result.assign(std::istreambuf_iterator<char>{in},
                  std::istreambuf_iterator<char>{});
  };
  if (auto fname = caf::get_if<std::string>(&inv.options, file_option)) {
    // Sanity check.
    if (!inv.arguments.empty())
      return caf::make_error(ec::parse_error, "got a query on the command line "
                                              "but --read option is defined");
    // Read query from STDIN if file name is '-'.
    if (*fname == "-")
      assign_query(std::cin);
    else {
      std::ifstream f{*fname};
      if (!f)
        return caf::make_error(ec::no_such_file,
                               "unable to read from " + *fname);
      assign_query(f);
    }
  } else if (inv.arguments.empty()) {
    // Read query from STDIN.
    if (::isatty(::fileno(stdout)))
      std::cerr << "please enter a query and confirm with CTRL-D: "
                << std::flush;
    assign_query(std::cin);
  } else {
    // Assemble expression from all remaining arguments.
    if (inv.arguments.size() > 1) {
      VAST_WARN("spreading a query over multiple arguments is "
                "deprecated; please pass it as a single string "
                "instead.");
      VAST_VERBOSE("(hint: use a heredoc if you run into quoting "
                   "issues.)");
    }
    result = detail::join(inv.arguments.begin() + argument_offset,
                          inv.arguments.end(), " ");
  }
  if (result.empty())
    return caf::make_error(ec::invalid_query);
  return result;
}

} // namespace vast::system
