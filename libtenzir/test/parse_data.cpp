//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/test/test.hpp"

#include <string_view>

using namespace tenzir;
using namespace std::string_literals;

namespace {

data to_data(std::string_view str) {
  data x;
  if (! parsers::data(str, x)) {
    FAIL("failed to parse data from {}", str);
  }
  return x;
}

} // namespace

TEST("data") {
  MESSAGE("null");
  CHECK_EQUAL(to_data("null"), caf::none);
  MESSAGE("bool");
  CHECK_EQUAL(to_data("true"), data{true});
  CHECK_EQUAL(to_data("false"), data{false});
  MESSAGE("int");
  CHECK_EQUAL(to_data("+42"), int64_t{42});
  CHECK_EQUAL(to_data("-42"), int64_t{-42});
  CHECK_EQUAL(to_data("-42k"), int64_t{-42'000});
  MESSAGE("count");
  CHECK_EQUAL(to_data("42"), data{42u});
  CHECK_EQUAL(to_data("42M"), data{42'000'000u});
  CHECK_EQUAL(to_data("42Ki"), data{42 * 1024u});
  MESSAGE("real");
  CHECK_EQUAL(to_data("4.2"), data{4.2});
  CHECK_EQUAL(to_data("-0.1"), data{-0.1});
  MESSAGE("string");
  CHECK_EQUAL(to_data("\"foo\""), data{"foo"});
  MESSAGE("pattern");
  CHECK_EQUAL(to_data("/foo/"), unbox(to<pattern>("/foo/")));
  MESSAGE("IP address");
  CHECK_EQUAL(to_data("10.0.0.1"), unbox(to<ip>("10.0.0.1")));
  MESSAGE("list");
  CHECK_EQUAL(to_data("[]"), list{});
  CHECK_EQUAL(to_data("[42, 4.2, null]"), (list{42u, 4.2, caf::none}));
  MESSAGE("map");
  CHECK_EQUAL(to_data("{}"), map{});
  CHECK_EQUAL(to_data("{+1->true,+2->false}"),
              (map{{int64_t{1}, true}, {int64_t{2}, false}}));
  CHECK_EQUAL(to_data("{-1 -> true, -2 -> false}"),
              (map{{int64_t{-1}, true}, {int64_t{-2}, false}}));
  MESSAGE("record - named fields");
  CHECK_EQUAL(to_data("<>"), record{});
  CHECK_EQUAL(to_data("<foo: 1>"), (record{{"foo", 1u}}));
  CHECK_EQUAL(to_data("<foo: 1, bar: 2>"), (record{{"foo", 1u}, {"bar", 2u}}));
  CHECK_EQUAL(to_data("<foo: 1, bar: <baz: 3>>"),
              (record{{"foo", 1u}, {"bar", record{{"baz", 3u}}}}));
  MESSAGE("record - ordered fields");
  CHECK_EQUAL(to_data("<1>"),
              record::make_unsafe(record::vector_type{{"", 1u}}));
  CHECK_EQUAL(to_data("<_>"),
              record::make_unsafe(record::vector_type{{"", caf::none}}));
  CHECK_EQUAL(to_data("<_, /foo/>"),
              record::make_unsafe(record::vector_type{
                {"", caf::none}, {"", unbox(to<pattern>("/foo/"))}}));
}
