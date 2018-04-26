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

#define SUITE option_declaration_set
#include "test.hpp"

#include "vast/option_declaration_set.hpp"

using namespace vast;

namespace {

struct fixture {
  option_declaration_set decl;
};

} // namespace <anonymous>

FIXTURE_SCOPE(option_declaration_set_tests, fixture)

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
  using parse_state = option_declaration_set::parse_state;
  auto check_option
    = [](const option& opt, const std::string& str, auto expected_value) {
        auto [state, x] = opt.parse(str);
        CHECK_EQUAL(state, parse_state::successful);
        CHECK_EQUAL(x, expected_value);
      };
  auto check_fail_option = [](const option& opt, const std::string& str) {
    auto [state, x] = opt.parse(str);
    CHECK_NOT_EQUAL(state, parse_state::successful);
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
  check_option(*x, "\"2\"", "2");
  check_option(*x, "\"this is a test\"", "this is a test");
}

FIXTURE_SCOPE_END()
