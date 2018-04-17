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
    add_opt("value,v", "Some integer value", value);
    add_opt("flag", "Some flag", flag);
  }

  proceed_result proceed(caf::actor_system&, option_map&, const_iterator,
                         const_iterator) override {
    tested_proceed = true;
    return proceed_ok;
  }

  int run_impl(caf::actor_system&, option_map&, const_iterator,
               const_iterator) override {
    was_executed = true;
    return EXIT_SUCCESS;
  }

  int value = 0;
  bool flag = false;
  bool tested_proceed = false;
  bool was_executed = false;
};

class bar : public command {
public:
  bar(command* parent, std::string_view name) : command(parent, name) {
    add_opt("other-value,o", "Some other integer value", other_value);
  }

  proceed_result proceed(caf::actor_system&, option_map&, const_iterator,
                         const_iterator) override {
    tested_proceed = true;
    return proceed_ok;
  }

  int run_impl(caf::actor_system&, option_map&, const_iterator args_begin,
               const_iterator args_end) override {
    was_executed = true;
    begin = args_begin;
    end = args_end;
    return EXIT_SUCCESS;
  }

  int other_value = 0;
  bool tested_proceed = false;
  bool was_executed = false;
  const_iterator begin;
  const_iterator end;
};

struct fixture {
  command root;
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  command::option_map options;
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

TEST(parsing value) {
  auto cmd = root.add<foo>("foo");
  exec("foo -v 42");
  CHECK_EQUAL(cmd->flag, false);
  CHECK_EQUAL(cmd->value, 42);
  CHECK_EQUAL(caf::deep_to_string(options),
              R"([("flag", false), ("value", 42)])");
}

TEST(parsing flag) {
  auto cmd = root.add<foo>("foo");
  exec("foo --flag");
  CHECK_EQUAL(cmd->flag, true);
  CHECK_EQUAL(cmd->value, 0);
  CHECK_EQUAL(caf::deep_to_string(options),
              R"([("flag", true), ("value", 0)])");
}

TEST(parsing both) {
  auto cmd = root.add<foo>("foo");
  exec("foo --flag -v 42");
  CHECK_EQUAL(cmd->flag, true);
  CHECK_EQUAL(cmd->value, 42);
  CHECK_EQUAL(caf::deep_to_string(options),
              R"([("flag", true), ("value", 42)])");
}

TEST(nested arg parsing) {
  auto cmd1 = root.add<foo>("foo");
  auto cmd2 = cmd1->add<bar>("bar");
  exec("foo -v 42 bar -o 123 '--this should not -be parsed ! x'");
  CHECK_EQUAL(cmd1->value, 42);
  CHECK_EQUAL(cmd2->other_value, 123);
  CHECK_EQUAL(caf::deep_to_string(options),
              R"([("flag", false), ("other-value", 123), ("value", 42)])");
  CHECK_EQUAL(cmd1->tested_proceed, true);
  CHECK_EQUAL(cmd1->was_executed, false);
  CHECK_EQUAL(cmd2->tested_proceed, true);
  REQUIRE(cmd2->was_executed);
  std::string str;
  if (cmd2->begin != cmd2->end)
    str = std::accumulate(
      std::next(cmd2->begin), cmd2->end, *cmd2->begin,
      [](std::string a, const std::string& b) { return a += ' ' + b; });
  CHECK_EQUAL(str, "'--this should not -be parsed ! x'");
}

FIXTURE_SCOPE_END()
