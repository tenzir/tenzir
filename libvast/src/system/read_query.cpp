//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/read_query.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
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
  VAST_TRACE_SCOPE("{} {}", inv, file_option);
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
  } else if (inv.arguments.size() <= argument_offset) {
    VAST_VERBOSE("not providing a query causes everything to be exported; "
                 "please be aware that this operation may be very expensive.");
    result = "#type != \"this expression matches everything\"";
  } else if (inv.arguments.size() == argument_offset + 1) {
    result = inv.arguments[argument_offset];
  } else {
    VAST_ERROR("spreading a query over multiple arguments is "
               "not allowed; please pass it as a single string "
               "instead.");
  }
  return result;
}

} // namespace vast::system
