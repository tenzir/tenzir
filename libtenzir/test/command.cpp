//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/command.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/variant_traits.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/make_message.hpp>
#include <caf/message.hpp>

using namespace tenzir;

using namespace std::string_literals;

namespace {

caf::message foo(const invocation& inv, caf::actor_system&) {
  CHECK_EQUAL(inv.name(), "foo");
  return caf::make_message("foo");
}

caf::message bar(const invocation& inv, caf::actor_system&) {
  CHECK_EQUAL(inv.name(), "bar");
  return caf::make_message("bar");
}

struct fixture {
  command root;
  caf::actor_system_config cfg;
  caf::actor_system sys{cfg};
  invocation inv;

  fixture() : root{"tenzir", "", command::opts()} {
  }

  caf::variant<caf::none_t, std::string, caf::error>
  exec(std::string str, const command::factory& factory) {
    inv.options.clear();
    std::vector<std::string> xs;
    caf::split(xs, str, ' ', caf::token_compress_on);
    auto expected_inv = parse(root, xs.begin(), xs.end());
    if (!expected_inv)
      return expected_inv.error();
    inv = std::move(*expected_inv);
    auto result = run(inv, sys, factory);
    if (!result)
      return result.error();
    if (result->empty())
      return caf::none;
    if (result->match_elements<std::string>())
      return result->get_as<std::string>(0);
    if (result->match_elements<caf::error>())
      return result->get_as<caf::error>(0);
    FAIL("command returned an unexpected result");
  }
};

template <class T>
bool is_error(const T& x) {
  return is<caf::error>(x);
}

} // namespace

FIXTURE_SCOPE(command_tests, fixture)

TEST(names) {
  using svec = std::vector<std::string>;
  auto aa = root.add_subcommand("a", "", command::opts())
              ->add_subcommand("aa", "", command::opts());
  aa->add_subcommand("aaa", "", command::opts());
  aa->add_subcommand("aab", "", command::opts());
  CHECK_EQUAL(aa->name, "aa");
  root.add_subcommand("b", "", command::opts());
  svec names;
  for_each(root, [&](auto& cmd) { names.emplace_back(cmd.full_name()); });
  CHECK_EQUAL(names,
              svec({"tenzir", "a", "a aa", "a aa aaa", "a aa aab", "b"}));
}

TEST(flat command invocation) {
  auto factory = command::factory{
    {"foo", foo},
    {"bar", bar},
  };
  auto fptr = root.add_subcommand("foo", "",
                                  command::opts()
                                    .add<int>("value,v", "some int")
                                    .add<bool>("flag", "some flag"));
  CHECK_EQUAL(fptr->name, "foo");
  CHECK_EQUAL(fptr->full_name(), "foo");
  auto bptr = root.add_subcommand("bar", "", command::opts());
  CHECK_EQUAL(bptr->name, "bar");
  CHECK_EQUAL(bptr->full_name(), "bar");
  CHECK(is_error(exec("nop", factory)));
  CHECK(is_error(exec("bar --flag -v 42", factory)));
  CHECK(is_error(exec("--flag bar", factory)));
  CHECK_EQUAL(get_or(inv.options, "flag", false), false);
  CHECK_EQUAL(get_or(inv.options, "value", 0), 0);
  CHECK_VARIANT_EQUAL((exec("bar", factory)), "bar"s);
  CHECK_VARIANT_EQUAL((exec("foo --flag -v 42", factory)), "foo"s);
  CHECK_EQUAL(get_or(inv.options, "flag", false), true);
  CHECK_EQUAL(get_or(inv.options, "value", 0), 42);
}

TEST(nested command invocation) {
  auto factory = command::factory{
    {"foo", foo},
    {"foo bar", bar},
  };
  auto fptr = root.add_subcommand("foo", "",
                                  command::opts()
                                    .add<int>("value,v", "some int")
                                    .add<bool>("flag", "some flag"));
  CHECK_EQUAL(fptr->name, "foo");
  CHECK_EQUAL(fptr->full_name(), "foo");
  auto bptr = fptr->add_subcommand("bar", "", command::opts());
  CHECK_EQUAL(bptr->name, "bar");
  CHECK_EQUAL(bptr->full_name(), "foo bar");
  CHECK(is_error(exec("nop", factory)));
  CHECK(is_error(exec("bar --flag -v 42", factory)));
  CHECK(is_error(exec("foo --flag -v 42 --other-flag", factory)));
  CHECK_VARIANT_EQUAL(exec("foo --flag -v 42", factory), "foo"s);
  CHECK_EQUAL(get_or(inv.options, "flag", false), true);
  CHECK_EQUAL(get_or(inv.options, "value", 0), 42);
  CHECK_VARIANT_EQUAL(exec("foo --flag -v 42 bar", factory), "bar"s);
  CHECK_EQUAL(get_or(inv.options, "flag", false), true);
  CHECK_EQUAL(get_or(inv.options, "value", 0), 42);
  // Setting the command function to nullptr prohibits calling it directly.
  factory.erase(fptr->full_name());
  CHECK(is_error(exec("foo --flag -v 42", factory)));
  // Subcommands of course still work.
  CHECK_VARIANT_EQUAL(exec("foo --flag -v 42 bar", factory), "bar"s);
}

TEST(missing argument) {
  auto factory = command::factory{
    {"foo", foo},
  };
  root.add_subcommand("foo", "",
                      command::opts().add<int>("value,v", "some int"));
  CHECK(is_error(exec("foo -v", factory)));
}

FIXTURE_SCOPE_END()
