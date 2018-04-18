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

#include "vast/detail/string.hpp"

using namespace vast;

namespace {

struct fixture {
  option_declaration_set decl;
  option_map opts;
};

} // namespace <anonymous>

FIXTURE_SCOPE(command_tests, fixture)

TEST(declaration adding) {
  CHECK(decl.add("flag,fabc", "this is a flag", false));  
  CHECK(decl.add("str,s", "this is a string", std::string("")));  
  CHECK(decl.add("test-int", "this is an int", 1));  
  CHECK(!decl.add(",x", "using only a short name", 1));  
  CHECK(!decl.add("flag", "using the same long name again", false));  
  CHECK_EQUAL(decl.size(), 3u + 1u); //3 options added + the help option
  auto x = decl.find("help");
  MESSAGE("Test help option");
  CHECK_EQUAL(x->long_name(), "help");
  REQUIRE_EQUAL(x->short_names().size(), 2u);
  CHECK_EQUAL(x->short_names()[0], 'h');
  CHECK_EQUAL(x->short_names()[1], '?');
  CHECK_EQUAL(x->description(), "print this text");
  CHECK_EQUAL(x->has_argument(), false);
  REQUIRE(x);
  MESSAGE("Test flag option");
  x = decl.find("flag");
  REQUIRE(x);
  CHECK_EQUAL(x->long_name(), "flag");
  REQUIRE_EQUAL(x->short_names().size(), 4u);
  CHECK_EQUAL(x->short_names()[0], 'f');
  CHECK_EQUAL(x->short_names()[3], 'c');
  CHECK_EQUAL(x->description(), "this is a flag");
  CHECK_EQUAL(x->has_argument(), false);
  MESSAGE("Test string option");
  x = decl.find("str");
  REQUIRE(x); 
  CHECK_EQUAL(x->long_name(), "str");
  CHECK_EQUAL(x->has_argument(), true);
}

TEST(data type parsing) {
  using option = option_declaration_set::option_declaration;
  auto check_option
    = [](const option& opt, const std::string& str, auto expected_value) {
        auto d = opt.parse(str);
        REQUIRE(d);
        CHECK_EQUAL(*d, expected_value);
      };
  auto check_fail_option = [](const option& opt, const std::string& str) {
    auto d = opt.parse(str);
    CHECK(!d);
  }; 
  CHECK(decl.add("int", "", 1));  
  CHECK(decl.add("string", "", ""));  
  MESSAGE("Test int");
  auto x = decl.find("int");
  check_option(*x, "2", 2);
  check_option(*x, "0", 0);
  check_option(*x, "-2", -2);
  check_fail_option(*x, "X");
  MESSAGE("Test string");
  x = decl.find("string");
  check_option(*x, "2", "2");
  check_option(*x, "this is a test", "this is a test");
}

TEST(option map handling) {
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

TEST(parse cli) {
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
    CHECK_EQUAL(state, option_declaration_set::parse_result::successful);
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
  CHECK_EQUAL(state, option_declaration_set::parse_result::successful);
  CHECK_EQUAL(it, args.end());
  check_option("boolean", false);
  check_option("integer", 1);
  check_option("string", "foo");
  check_fail_option("not-contained");
  MESSAGE("Test long names");
  args = split("--boolean --integer=42 --string=test");
  check_all_options(args);
  MESSAGE("Test shortnames");
  args = split("-b -i42 -s test");
  check_all_options(args);
  MESSAGE("Test mix of short_names and long_names");
  args = split("-b -i 42 --string=test");
  check_all_options(args);
  MESSAGE("Test two option declaration sets");
  option_declaration_set decl2;
  CHECK(decl2.add("boolean2,b", "", false));  
  CHECK(decl2.add("integer2,i", "", 2));  
  CHECK(decl2.add("string2,s", "", "bar"));  
  opts.clear();
  args = split("--boolean --integer=42 --string=test");
  std::tie(state, it) = decl.parse(opts, args.begin(), args.end());
  CHECK_EQUAL(state, option_declaration_set::parse_result::successful);
  CHECK_EQUAL(it, args.end());
  args = split("--integer2=1337 -stest2");
  std::tie(state, it) = decl2.parse(opts, args.begin(), args.end());
  std::cerr << "state:::" << static_cast<int>(state) << std::endl;
  CHECK_EQUAL(state, option_declaration_set::parse_result::successful);
  CHECK_EQUAL(it, args.end());
  check_option("boolean", true);
  check_option("boolean2", false);
  check_option("integer", 42);
  check_option("integer2", 1337);
  check_option("string", "test");
  check_option("string2", "test2");
}

FIXTURE_SCOPE_END()
