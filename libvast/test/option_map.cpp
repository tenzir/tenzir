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

#define SUITE option_map
#include "test.hpp"

#include <limits>

#include "vast/option_map.hpp"
#include "vast/option_declaration_set.hpp"

using namespace vast;

namespace {

struct fixture {
  option_declaration_set decl;
  option_map opts;
};

} // namespace <anonymous>

FIXTURE_SCOPE(command_tests, fixture)

TEST(retrieving data) {
  auto num = 42;
  opts.add("true", true);
  opts.add("false", false);
  CHECK(!opts.add("true", true));
  CHECK_EQUAL(opts.size(), 2u);
  auto x = opts.get("true");
  REQUIRE(x);
  CHECK_EQUAL(get<boolean>(*x), true);
  x = opts.get("false");
  REQUIRE(x);
  CHECK_EQUAL(get<boolean>(*x), false);
  opts.set("true", false);
  x = opts.get("true");
  REQUIRE(x);
  CHECK_EQUAL(get<boolean>(*x), false);
  x = opts.get("number");
  CHECK(!x);
  auto y = opts.get_or("number", num);
  CHECK_EQUAL(y, num);
}

TEST(cli parsing) {
  auto split = [](const std::string& str) {
    std::vector<std::string> result;
    auto xs = detail::split(str.begin(), str.end(), " ");
    for (auto& [begin, end] : xs) {
      result.emplace_back(std::string{begin, end});
    }
    return result;
  };
  auto check_option = [&](const std::string& name, auto value) {
    auto d = opts.get(name);
    REQUIRE(d);
    CHECK_EQUAL(*d, value);
  };
  auto check_fail_option = [&](const std::string& name) {
    auto d = opts.get(name);
    CHECK(!d);
  };
  auto check_all_options = [&](const auto& args) {
    opts.clear();
    auto [state, it] = decl.parse(opts, args.begin(), args.end());
    CHECK_EQUAL(state, option_declaration_set::parse_state::successful);
    CHECK_EQUAL(it, args.end());
    check_option("boolean", true);
    check_option("integer", 42);
    check_option("string", "test");
  };
  CHECK(decl.add("boolean,b", "", false));  
  CHECK(decl.add("integer,i", "", 1));  
  CHECK(decl.add("string,s", "", "foo"));  
  MESSAGE("Test default values");
  auto args = split("");
  auto [state, it] = decl.parse(opts, args.begin(), args.end());
  CHECK_EQUAL(state, option_declaration_set::parse_state::successful);
  CHECK_EQUAL(it, args.end());
  check_option("boolean", false);
  check_option("integer", 1);
  check_option("string", "foo");
  check_fail_option("not-contained");
  MESSAGE("Test long names");
  args = split("--boolean --integer=42 --string=\"test\"");
  check_all_options(args);
  MESSAGE("Test shortnames");
  args = split("-b -i42 -s \"test\"");
  check_all_options(args);
  MESSAGE("Test mix of short_names and long_names");
  args = split("-b -i 42 --string=\"test\"");
  check_all_options(args);
  MESSAGE("Test two option declaration sets");
  option_declaration_set decl2;
  CHECK(decl2.add("boolean2,b", "", false));  
  CHECK(decl2.add("integer2,i", "", 2));  
  CHECK(decl2.add("string2,s", "", "bar"));  
  opts.clear();
  args = split("--boolean --integer=42 --string=\"test\"");
  std::tie(state, it) = decl.parse(opts, args.begin(), args.end());
  CHECK_EQUAL(state, option_declaration_set::parse_state::successful);
  CHECK_EQUAL(it, args.end());
  args = split("--integer2=1337 -s\"test2\"");
  std::tie(state, it) = decl2.parse(opts, args.begin(), args.end());
  CHECK_EQUAL(state, option_declaration_set::parse_state::successful);
  CHECK_EQUAL(it, args.end());
  check_option("boolean", true);
  check_option("boolean2", false);
  check_option("integer", 42);
  check_option("integer2", 1337);
  check_option("string", "test");
  check_option("string2", "test2");
}

FIXTURE_SCOPE_END()
