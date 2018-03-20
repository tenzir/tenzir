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

  int value = 0;
  bool flag = false;
};

class bar : public command {
public:
  bar(command* parent, std::string_view name) : command(parent, name) {
    add_opt("other-value,o", "Some other integer value", other_value);
  }

  int other_value = 0;
};

struct fixture {
  command root;
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  command::opt_map options;
  int exec(std::string str) {
    caf::message_builder mb;
    std::vector<std::string> xs;
    caf::split(xs, str, ' ', caf::token_compress_on);
    for (auto& x : xs)
      mb.append(std::move(x));
    return root.run(sys, options, mb.move_to_message());
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
  exec("foo -v 42 bar -o 123");
  CHECK_EQUAL(cmd1->value, 42);
  CHECK_EQUAL(cmd2->other_value, 123);
  CHECK_EQUAL(caf::deep_to_string(options),
              R"([("flag", false), ("other-value", 123), ("value", 42)])");
}

FIXTURE_SCOPE_END()
