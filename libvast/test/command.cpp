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

#include "vast/command.hpp"

#define SUITE command
#include "test.hpp"

using namespace vast;

class foo : public command {
public:
  foo(command* parent, std::string_view name) : command(parent, name) {
    add_opt("value,v", "Some integer value", value);
  }

  int value;
};

TEST(full name) {
  command root;
  auto cmd1 = root.add<foo>("foo");
  auto cmd2 = cmd1->add<command>("bar");
  CHECK_EQUAL(cmd2->full_name(), "foo bar");
}

TEST(arg parsing) {
  command root;
  auto cmd = root.add<foo>("foo");
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  command::opt_map om;
  root.run(sys, om, caf::make_message("foo", "-v", "42"));
  CHECK_EQUAL(cmd->value, 42);
  CHECK_EQUAL(caf::deep_to_string(om), R"([("value", 42)])");
}
/*
TEST(command) {
  command cmd;
  cmd
    .opt("example,e", "a full option with value", "x")
    .opt("flag,f", "print version and exit")
    .opt("long", "a boolean long flag")
    .callback([](const command& cmd, std::vector<std::string> args) {
      // TODO
    });
  // TODO
  //cmd.dispatch();
}
*/
