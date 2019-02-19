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

#include "vast/test/test.hpp"

#include "vast/command.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>

#include "vast/system/version_command.hpp"

using namespace vast;

using namespace std::string_literals;

namespace {

caf::message foo(const command& cmd, caf::actor_system&, caf::settings&,
                 command::argument_iterator, command::argument_iterator) {
  CHECK_EQUAL(cmd.name, "foo");
  return caf::make_message("foo");
}

caf::message bar(const command& cmd, caf::actor_system&, caf::settings&,
                 command::argument_iterator, command::argument_iterator) {
  CHECK_EQUAL(cmd.name, "bar");
  return caf::make_message("bar");
}

struct fixture {
  command root;
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  caf::settings options;

  fixture() {
    root.name = "vast";
  }

  caf::variant<caf::none_t, std::string, caf::error> exec(std::string str) {
    options.clear();
    std::vector<std::string> xs;
    caf::split(xs, str, ' ', caf::token_compress_on);
    auto result = run(root, sys, options, xs.begin(), xs.end());
    if (result.empty())
      return caf::none;
    if (result.match_elements<std::string>())
      return result.get_as<std::string>(0);
    if (result.match_elements<caf::error>())
      return result.get_as<caf::error>(0);
    FAIL("command returned an unexpected result");
  }
};

template <class T>
bool is_error(const T& x) {
  return caf::holds_alternative<caf::error>(x);
}

} // namespace <anonymous>

FIXTURE_SCOPE(command_tests, fixture)

TEST(names) {
  using svec = std::vector<std::string>;
  auto aa = root.add(nullptr, "a", "")->add(nullptr, "aa", "");
  aa->add(nullptr, "aaa", "");
  aa->add(nullptr, "aab", "");
  CHECK_EQUAL(aa->name, "aa");
  root.add(nullptr, "b", "");
  svec names;
  for_each(root, [&](auto& cmd) { names.emplace_back(full_name(cmd)); });
  CHECK_EQUAL(names, svec({"vast", "vast a", "vast a aa", "vast a aa aaa",
                           "vast a aa aab", "vast b"}));
}

TEST(flat command invocation) {
  auto fptr = root.add(foo, "foo", "",
                       command::opts()
                         .add<int>("value,v", "some int")
                         .add<bool>("flag", "some flag"));
  CHECK_EQUAL(fptr->name, "foo");
  CHECK_EQUAL(full_name(*fptr), "vast foo");
  auto bptr = root.add(bar, "bar", "");
  CHECK_EQUAL(bptr->name, "bar");
  CHECK_EQUAL(full_name(*bptr), "vast bar");
  CHECK(is_error(exec("nop")));
  CHECK(is_error(exec("bar --flag -v 42")));
  CHECK(is_error(exec("--flag bar")));
  CHECK_EQUAL(get_or(options, "flag", false), false);
  CHECK_EQUAL(get_or(options, "value", 0), 0);
  CHECK_EQUAL(exec("bar"), "bar"s);
  CHECK_EQUAL(exec("foo --flag -v 42"), "foo"s);
  CHECK_EQUAL(get_or(options, "flag", false), true);
  CHECK_EQUAL(get_or(options, "value", 0), 42);
}

TEST(nested command invocation) {
  auto fptr = root.add(foo, "foo", "",
                       command::opts()
                         .add<int>("value,v", "some int")
                         .add<bool>("flag", "some flag"));
  CHECK_EQUAL(fptr->name, "foo");
  CHECK_EQUAL(full_name(*fptr), "vast foo");
  auto bptr = fptr->add(bar, "bar", "");
  CHECK_EQUAL(bptr->name, "bar");
  CHECK_EQUAL(full_name(*bptr), "vast foo bar");
  CHECK(is_error(exec("nop")));
  CHECK(is_error(exec("bar --flag -v 42")));
  CHECK(is_error(exec("foo --flag -v 42 --other-flag")));
  CHECK_EQUAL(exec("foo --flag -v 42"), "foo"s);
  CHECK_EQUAL(get_or(options, "flag", false), true);
  CHECK_EQUAL(get_or(options, "value", 0), 42);
  CHECK_EQUAL(exec("foo --flag -v 42 bar"), "bar"s);
  CHECK_EQUAL(get_or(options, "flag", false), true);
  CHECK_EQUAL(get_or(options, "value", 0), 42);
  // Setting the command function to nullptr prohibits calling it directly.
  fptr->run = nullptr;
  CHECK(is_error(exec("foo --flag -v 42")));
  // Subcommands of course still work.
  CHECK_EQUAL(exec("foo --flag -v 42 bar"), "bar"s);
}

TEST(version command) {
  root.add(system::version_command, "version", "", command::opts());
  CHECK_EQUAL(exec("version"), caf::none);
}

FIXTURE_SCOPE_END()
