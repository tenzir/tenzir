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
           enum must_provide_query must_provide_query, size_t argument_offset) {
  VAST_TRACE_SCOPE("{} {}", inv, file_option);
  std::string result;
  auto assign_query = [&](std::istream& in) {
    result.assign(std::istreambuf_iterator<char>{in},
                  std::istreambuf_iterator<char>{});
  };
  // The below logic matches the following behavior:
  // vast export -r ... <format> <query>
  //   errors.
  // vast export <format> <query>
  //   takes the query from the command line, and errors if a query is also
  //   present on stdin.
  // vast export -r - <format>
  //   reads the query from stdin.
  // vast export <format>
  //   reads the query from stdin, and if none is present, it exports everything
  const auto fname = caf::get_if<std::string>(&inv.options, file_option);
  const bool has_query_cli = inv.arguments.size() > argument_offset;
  const bool has_excess_cli = inv.arguments.size() > argument_offset + 1;
  const bool has_query_stdin = ::isatty(::fileno(stdin)) == 0;
  if (has_excess_cli) {
    VAST_ERROR("spreading a query over multiple arguments is "
               "not allowed; please pass it as a single string "
               "instead.");
  } else if (has_query_cli) {
    if (fname)
      return caf::make_error(ec::invalid_argument,
                             "got a query on the command line "
                             "but --read option is defined");
    if (has_query_stdin)
      return caf::make_error(ec::invalid_argument, "got a query on stdin and "
                                                   "the command line");
    result = inv.arguments[argument_offset];
  } else if (fname) {
    if (*fname == "-") {
      assign_query(std::cin);
    } else {
      if (has_query_stdin)
        return caf::make_error(ec::invalid_argument,
                               fmt::format("got a query on stdin, but "
                                           "--read option is set to '{}'",
                                           *fname));
      std::ifstream f{*fname};
      if (!f)
        return caf::make_error(ec::no_such_file,
                               fmt::format("unable to read from '{}'", *fname));
      assign_query(f);
    }
  } else if (has_query_stdin) {
    assign_query(std::cin);
  } else {
    switch (must_provide_query) {
      case must_provide_query::yes:
        return caf::make_error(ec::invalid_argument, "no query provided, but "
                                                     "command requires a query "
                                                     "argument");
      case must_provide_query::no:
        VAST_VERBOSE("not providing a query causes everything to be exported; "
                     "please be aware that this operation may be very "
                     "expensive.");
        result = "#type != \"this expression matches everything\"";
        break;
    }
  }
  return result;
}

} // namespace vast::system
