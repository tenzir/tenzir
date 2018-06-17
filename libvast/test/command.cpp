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

#define SUITE command
#include "test.hpp"

#include <caf/message_builder.hpp>
#include <caf/string_algorithms.hpp>

#include "vast/command.hpp"

using namespace vast;

namespace {

class foo : public command {
public:
  foo(command* parent, std::string_view name) : command(parent, name) {
    add_opt<int>("value,v", "Some integer value");
    add_opt<bool>("flag", "Some flag");
  }

  proceed_result proceed(caf::actor_system&, const caf::config_value_map&,
                         argument_iterator begin,
                         argument_iterator end) override {
    tested_proceed = true;
    proceed_begin = begin;
    proceed_end = end;
    return proceed_ok;
  }

  int run_impl(caf::actor_system&, const caf::config_value_map&,
               argument_iterator begin, argument_iterator end) override {
    was_executed = true;
    run_begin = begin;
    run_end = end;
    return EXIT_SUCCESS;
  }

  bool tested_proceed = false;
  bool was_executed = false;
  argument_iterator proceed_begin;
  argument_iterator proceed_end;
  argument_iterator run_begin;
  argument_iterator run_end;
};

class bar : public command {
public:
  bar(command* parent, std::string_view name) : command(parent, name) {
    add_opt<int>("other-value,o", "Some other integer value");
  }

  proceed_result proceed(caf::actor_system&, const caf::config_value_map&,
                         argument_iterator begin,
                         argument_iterator end) override {
    tested_proceed = true;
    proceed_begin = begin;
    proceed_end = end;
    return proceed_ok;
  }

  int run_impl(caf::actor_system&, const caf::config_value_map&,
               argument_iterator begin, argument_iterator end) override {
    was_executed = true;
    run_begin = begin;
    run_end = end;
    return EXIT_SUCCESS;
  }

  bool tested_proceed = false;
  bool was_executed = false;
  argument_iterator proceed_begin;
  argument_iterator proceed_end;
  argument_iterator run_begin;
  argument_iterator run_end;
};

struct fixture {
  command root;
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  caf::config_value_map options;
  std::vector<std::string> xs;
  int exec(std::string str) {
    caf::split(xs, str, ' ', caf::token_compress_on);
    return root.run(sys, options, xs.begin(), xs.end());
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(command_tests, fixture)

TEST(full name) {
  auto cmd1 = root.add<foo>("foo");
  auto cmd2 = cmd1->add<command>("bar");
  CHECK_EQUAL(cmd2->full_name(), "foo bar");
}

TEST(parsing args) {
  root.add<foo>("foo");
  exec("foo --flag -v 42");
  CHECK_EQUAL(get_or(options, "flag", false), true);
  CHECK_EQUAL(get_or(options, "value", 0), 42);
}

TEST(nested arg parsing) {
  auto cmd1 = root.add<foo>("foo");
  cmd1->add<bar>("bar");
  exec("foo -v 42 bar -o 123");
  CHECK_EQUAL(get_or(options, "flag", false), false);
  CHECK_EQUAL(get_or(options, "value", 0), 42);
  CHECK_EQUAL(get_or(options, "other-value", 0), 123);
}

TEST(parsing arg remainder) {
  auto cmd1 = root.add<foo>("foo");
  auto cmd2 = cmd1->add<bar>("bar");
  exec("foo -v 42 bar -o 123 '--this should not -be parsed ! x'");
  CHECK_EQUAL(cmd1->tested_proceed, true);
  CHECK_EQUAL(cmd1->was_executed, false);
  CHECK_EQUAL(cmd2->tested_proceed, true);
  REQUIRE(cmd2->was_executed);
  CHECK_EQUAL(cmd2->proceed_begin, cmd2->run_begin);
  CHECK_EQUAL(cmd2->proceed_end, cmd2->run_end);
  std::string str;
  if (cmd2->run_begin != cmd2->run_end)
    str = std::accumulate(
      std::next(cmd2->run_begin), cmd2->run_end, *cmd2->run_begin,
      [](std::string a, const std::string& b) { return a += ' ' + b; });
  CHECK_EQUAL(str, "'--this should not -be parsed ! x'");
}

FIXTURE_SCOPE_END()
