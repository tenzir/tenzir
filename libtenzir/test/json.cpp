//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/error.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;
using namespace std::chrono_literals;
using namespace std::string_literals;

TEST("from_json - basic") {
  auto json = unbox(from_json(
    R"_({"a": 4.2, "b": -2, "c": 3, "d": null, "e": true, "f": "foo"})_"));
  CHECK_EQUAL(json, (record{{{"a", 4.2},
                             {"b", int64_t{-2}},
                             {"c", int64_t{3}},
                             {"d", data{}},
                             {"e", data{true}},
                             {"f", data{"foo"}}}}));
}

TEST("from_json - nested") {
  auto json
    = unbox(from_json(R"_({"a": {"inner": 4.2}, "b": ["foo", "bar"]})_"));
  CHECK_EQUAL(
    json, (record{{"a", record{{"inner", 4.2}}}, {"b", list{"foo", "bar"}}}));
}

TEST("from_json - invalid json") {
  auto json = from_json("@!#$%^&*()_+");
  REQUIRE(! json);
  CHECK_EQUAL(json.error(), ec::parse_error);
}
