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

caf::expected<std::string>
make_all_query(enum must_provide_query must_provide_query) {
  switch (must_provide_query) {
    case must_provide_query::yes:
      return caf::make_error(ec::invalid_argument, "no query provided, but "
                                                   "command requires a query "
                                                   "argument");
    case must_provide_query::no:
      VAST_VERBOSE("not providing a query causes everything to be exported; "
                   "please be aware that this operation may be very "
                   "expensive.");
      return R"__(#type != "this expression matches everything")__";
  }
  die("unreachable");
}

} // namespace

caf::expected<std::string>
read_query(const invocation& inv, std::string_view file_option,
           enum must_provide_query must_provide_query, size_t argument_offset) {
  VAST_TRACE_SCOPE("{} {}", inv, file_option);
  auto ambiguous
    = caf::make_error(ec::invalid_argument, "got a query on the command line "
                                            "but --read option is defined");
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
  const bool has_query_stdin = ::isatty(::fileno(stdin)) == 0;
  //      |r
  // f   s|e
  // n   t|s
  // a c d|u
  // m l i|l
  // e i n|t
  // -----|-
  // -- read option unused ----------------------------------------------------
  // clang-format off
  if (!fname) {
  // 0 0 0|match all
    if (!has_query_cli && !has_query_stdin)
      return make_all_query(must_provide_query);
  // 0 0 1|query from stdin
    if (!has_query_cli && has_query_stdin)
      return read_query(std::cin);
  // 0 1 0|query from command line
    if (has_query_cli && !has_query_stdin)
      return read_query(inv.arguments, argument_offset);
  // 0 1 1|error
    VAST_ASSERT(has_query_cli && has_query_stdin);
    return ambiguous;
  }
  // -- explicitly read from stdin --------------------------------------------
  if (*fname == "-") {
  // - 0 0|query from stdin
  // - 0 1|query from stdin
    if (!has_query_cli)
      return read_query(std::cin);
  // - 1 0|error
  // - 1 1|error
    return ambiguous;
  }
  // clang-format on
  // -- read from file --------------------------------------------------------
  // 1 0 0|query from file
  if (!has_query_cli && !has_query_stdin)
    return read_query(*fname);
  // 1 0 1|error
  if (!has_query_cli && has_query_stdin)
    return caf::make_error(ec::invalid_argument,
                           fmt::format("got a query on stdin, but --read "
                                       "option is set to '{}'",
                                       *fname));
  // 1 1 0|error
  // 1 1 1|error
  VAST_ASSERT(has_query_cli);
  return ambiguous;
}

} // namespace vast::system
