//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/error.hpp"
#include "tenzir/json_parser.hpp"
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
  REQUIRE(not json);
  CHECK_EQUAL(json.error(), ec::parse_error);
}

TEST("JSON diagnostic - locate invalid UTF-8 sequences") {
  for (auto invalid : {"\xff"s, "\x80"s, "\xc0\xaf"s, "\xed\xa0\x80"s,
                       "\xf4\x90\x80\x80"s, "\xe2\x82"s}) {
    auto source = "prefix "s + invalid;
    auto diag = json::with_surrounding_bytes(diagnostic::error("invalid UTF-8"),
                                             source, simdjson::UTF8_ERROR)
                  .done();
    REQUIRE_EQUAL(diag.notes.size(), 2u);
    CHECK(diag.notes[0].message.starts_with("context:\nprefix \\x"));
    CHECK(diag.notes[0].message.ends_with("\n       ^"));
  }
}

TEST("JSON diagnostic - bounded escaped context and aligned caret") {
  auto source = "hidden prefix\n\xc3\xa4\xff\nhidden suffix"s;
  auto diag = json::with_surrounding_bytes(diagnostic::error("invalid UTF-8"),
                                           source, simdjson::UTF8_ERROR, 3)
                .done();
  REQUIRE_EQUAL(diag.notes.size(), 2u);
  CHECK_EQUAL(diag.notes[0].message,
              "context:\n...\\x0A\\xC3\\xA4\\xFF\\x0Ah...\n               ^");
}

TEST("JSON diagnostic - missing position without invalid UTF-8") {
  for (auto error : {simdjson::UTF8_ERROR, simdjson::TAPE_ERROR}) {
    auto diag = json::with_surrounding_bytes(diagnostic::error("invalid JSON"),
                                             "valid UTF-8", error)
                  .done();
    CHECK(diag.notes.empty());
  }
}

TEST("JSON diagnostic - end-of-input position") {
  auto source = "abc"s;
  auto diag
    = json::with_surrounding_bytes(diagnostic::error("invalid JSON"), source,
                                   source.data() + source.size())
        .done();
  REQUIRE_EQUAL(diag.notes.size(), 2u);
  CHECK_EQUAL(diag.notes[0].message, "context:\nabc\n   ^");
}

TEST("JSON parser - invalid UTF-8 includes input context") {
  auto dh = collecting_diagnostic_handler{};
  auto parser = json::default_parser{"json", dh, {}, false};
  auto source = "{\"message\":\"before\xff"
                "after\"}"s;
  parser.parse(as_bytes(source));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), 1u);
  CHECK_EQUAL(diagnostics[0].severity, severity::error);
  REQUIRE_EQUAL(diagnostics[0].notes.size(), 3u);
  CHECK_EQUAL(diagnostics[0].notes[1].message,
              "context:\n{\"message\":\"before\\xFFafter\"}\n                  "
              "^");
}
