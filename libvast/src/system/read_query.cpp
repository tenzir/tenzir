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
#include <sys/stat.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <unistd.h>

using namespace caf;
using namespace std::chrono_literals;

namespace vast::system {

namespace {

caf::expected<std::string>
read_query(const std::vector<std::string>& args, size_t offset) {
  if (args.size() > offset + 1)
    return caf::make_error(ec::invalid_argument, "spreading a query over "
                                                 "multiple arguments is not "
                                                 "allowed; please pass it as a "
                                                 "single string instead.");
  return args[offset];
}

caf::expected<std::string> read_query(std::istream& in) {
  auto result = std::string{};
  result.assign(std::istreambuf_iterator<char>{in},
                std::istreambuf_iterator<char>{});
  return result;
}

caf::expected<std::string> read_query(const std::string& path) {
  std::ifstream f{path};
  if (!f)
    return caf::make_error(ec::no_such_file,
                           fmt::format("unable to read from '{}'", path));
  return read_query(f);
}

std::string make_all_query() {
  VAST_VERBOSE("not providing a query causes everything to be exported; please "
               "be aware that this operation may be very expensive.");
  return R"__(#type != "this expression matches everything")__";
}

} // namespace

caf::expected<std::string>
read_query(const invocation& inv, std::string_view file_option,
           enum must_provide_query must_provide_query, size_t argument_offset) {
  VAST_TRACE_SCOPE("{} {}", inv, file_option);
  // The below logic matches the following behavior:
  // vast export <format> <query>
  //   takes the query from the command line
  // vast export -r - <format>
  //   reads the query from stdin.
  // echo "query" | vast export <format>
  //   reads the query from stdin
  // vast <query.txt export <format>
  //   reads the query from `query.txt`
  // vast export <format>
  //   export everything
  // Specifying any two conflicting ways of reading the query
  // results in an error.
  const auto fname = caf::get_if<std::string>(&inv.options, file_option);
  const bool has_query_cli = inv.arguments.size() > argument_offset;
  const bool has_query_stdin = [] {
    struct stat stats = {};
    if (::fstat(::fileno(stdin), &stats) != 0)
      return false;
    return S_ISFIFO(stats.st_mode) || S_ISREG(stats.st_mode);
  }();
  if (fname) {
    if (has_query_cli)
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("got query '{}' on the command line and query file"
                    " '{}' specified via '--read' option",
                    read_query(inv.arguments, argument_offset), *fname));
    if (*fname == "-")
      return read_query(std::cin);
    if (has_query_stdin)
      return caf::make_error(ec::invalid_argument,
                             fmt::format("stdin is connected to a pipe or "
                                         "regular file and query file '{}'",
                                         " specified via '--read' option",
                                         *fname));
    return read_query(*fname);
  }
  if (has_query_cli) {
    if (has_query_stdin)
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("got query '{}' on the command line and '{}' via stdin",
                    read_query(inv.arguments, argument_offset),
                    read_query(std::cin)));
    return read_query(inv.arguments, argument_offset);
  }
  if (has_query_stdin)
    return read_query(std::cin);
  if (must_provide_query == must_provide_query::yes)
    return caf::make_error(ec::invalid_argument, "no query provided, but "
                                                 "command requires a query "
                                                 "argument");
  // No query provided, make a query that finds everything.
  return make_all_query();
}

} // namespace vast::system
