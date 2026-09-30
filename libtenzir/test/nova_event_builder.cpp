//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/event_builder.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/test/test.hpp"

#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto make_builder(diagnostic_handler& dh, EventBuilder::Settings settings = {})
  -> EventBuilder {
  auto builder = EventBuilder::make(std::move(settings), dh);
  REQUIRE(builder);
  return std::move(builder).unwrap();
}

/// Finishes `builder` and materializes its events.
auto finish(EventBuilder& builder) -> std::vector<data> {
  auto events = builder.finish();
  auto rows = Array<Data>{events.data};
  auto result = std::vector<data>{};
  for (auto i = storage::Index{0}; i < rows.length(); ++i) {
    result.push_back(materialize_legacy(rows.get(i)));
  }
  return result;
}

} // namespace

TEST("event builder infers numbers only when requested") {
  auto dh = null_diagnostic_handler{};
  for (auto infer_numbers : {false, true}) {
    auto settings = EventBuilder::Settings{};
    settings.infer_numbers = infer_numbers;
    auto builder = make_builder(dh, std::move(settings));
    auto event = builder.event();
    event.field("x").data_unparsed("42");
    event.field("x").data_unparsed("-3");
    event.field("l").list().data_unparsed("1.5");
    auto events = finish(builder);
    REQUIRE_EQUAL(events.size(), 1u);
    if (infer_numbers) {
      CHECK_EQUAL(events[0], (data{record{{"x", list{int64_t{42}, int64_t{-3}}},
                                          {"l", list{1.5}}}}));
    } else {
      CHECK_EQUAL(events[0],
                  (data{record{{"x", list{"42", "-3"}}, {"l", list{"1.5"}}}}));
    }
  }
}

TEST("event builder raw strings override numeric inference") {
  auto dh = null_diagnostic_handler{};
  auto settings = EventBuilder::Settings{};
  settings.raw = true;
  settings.infer_numbers = true;
  auto builder = make_builder(dh, std::move(settings));
  auto event = builder.event();
  event.field("x").data_unparsed("42");
  event.field("l").list().data_unparsed("1.5");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0], (data{record{{"x", "42"}, {"l", list{"1.5"}}}}));
}

TEST("event builder collects the values of a repeated key into a list") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(dh);
  auto event = builder.event();
  event.field("x").data(int64_t{0});
  event.field("x").data(int64_t{1});
  event.field("x").data(std::string_view{"2"});
  builder.event().field("x").data(int64_t{3});
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 2u);
  CHECK_EQUAL(events[0],
              (data{record{{"x", list{int64_t{0}, int64_t{1}, "2"}}}}));
  CHECK_EQUAL(events[1], (data{record{{"x", int64_t{3}}}}));
}

TEST("event builder keeps the values of a repeated key as they are") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(dh);
  auto event = builder.event();
  event.field("l").list().data(int64_t{1});
  event.field("l").list().data(int64_t{2});
  event.field("l").data(int64_t{3});
  event.field("r").record().field("a").data(int64_t{1});
  event.field("r").record().field("b").data(int64_t{2});
  event.field("n").null();
  event.field("n").data(int64_t{1});
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"l", list{list{int64_t{1}}, list{int64_t{2}}, int64_t{3}}},
      {"r", list{record{{"a", int64_t{1}}}, record{{"b", int64_t{2}}}}},
      {"n", list{caf::none, int64_t{1}}},
    }}));
}

TEST("event builder tracks repeated keys per record") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(dh);
  auto elements = builder.event().field("r").list();
  auto first = elements.record();
  first.field("a").data(int64_t{1});
  first.field("a").data(int64_t{2});
  auto second = elements.record();
  second.field("a").data(int64_t{3});
  second.field("a").data(int64_t{4});
  second.field("a").data(int64_t{5});
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0],
              (data{record{
                {"r", list{
                        record{{"a", list{int64_t{1}, int64_t{2}}}},
                        record{{"a", list{int64_t{3}, int64_t{4}, int64_t{5}}}},
                      }}}}));
}

TEST("event builder infers numbers only under an opted-in field") {
  for (auto raw : {false, true}) {
    for (auto infer_numbers : {false, true}) {
      auto dh = null_diagnostic_handler{};
      auto settings = EventBuilder::Settings{};
      settings.raw = raw;
      settings.infer_numbers = infer_numbers;
      settings.infer_unparsed_under = "attributes";
      auto builder = make_builder(dh, std::move(settings));
      auto event = builder.event();
      event.field("version").data_unparsed("001");
      auto attributes = event.field("attributes").record();
      attributes.field("number").data_unparsed("001");
      attributes.field("text").data(std::string_view{"001"});
      attributes.field("nested").record().field("number").data_unparsed("-42");
      attributes.field("values").list().data_unparsed("3.5");
      auto events = finish(builder);
      REQUIRE_EQUAL(events.size(), 1u);
      CHECK_EQUAL(events[0],
                  (data{record{
                    {"version", "001"},
                    {"attributes",
                     record{
                       {"number", raw ? data{"001"} : data{int64_t{1}}},
                       {"text", "001"},
                       {"nested", record{{"number", raw ? data{"-42"}
                                                        : data{int64_t{-42}}}}},
                       {"values", list{raw ? data{"3.5"} : data{3.5}}},
                     }},
                  }}));
    }
  }
}

TEST("event builder scopes selector inference to unparsed fields") {
  for (auto raw : {false, true}) {
    for (auto infer_numbers : {false, true}) {
      auto dh = null_diagnostic_handler{};
      auto settings = EventBuilder::Settings{};
      settings.policy = EventBuilder::SelectorPolicy{"schema", None{}};
      settings.raw = raw;
      settings.infer_numbers = infer_numbers;
      settings.infer_unparsed_under = "attributes";
      auto builder = make_builder(dh, std::move(settings));
      auto event = builder.event();
      event.field("schema").data(std::string_view{"missing"});
      event.field("vendor").data(std::string_view{"true"});
      event.field("product").data(std::string_view{"192.0.2.1"});
      event.field("version").data(std::string_view{"1s"});
      event.field("attributes").record().field("flag").data_unparsed("true");
      auto events = finish(builder);
      REQUIRE_EQUAL(events.size(), 1u);
      CHECK_EQUAL(
        events[0],
        (data{record{
          {"schema", "missing"},
          {"vendor", "true"},
          {"product", "192.0.2.1"},
          {"version", "1s"},
          {"attributes", record{{"flag", raw ? data{"true"} : data{true}}}},
        }}));
    }
  }
}

TEST("event builder promotes unflattened scalar prefixes into records") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("k").data_unparsed("1");
  event.field("k.x").data_unparsed("2");
  event.field("k.y").data_unparsed("3");
  event.field("nested.k").data_unparsed("4");
  event.field("nested.k.x").data_unparsed("5");
  event.field("repeated").data_unparsed("6");
  event.field("repeated").data_unparsed("7");
  event.field("repeated.x").data_unparsed("8");
  event.field("null").null();
  event.field("null.x").data_unparsed("9");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"k", record{{"", int64_t{1}}, {"x", int64_t{2}}, {"y", int64_t{3}}}},
      {"nested", record{{"k", record{{"", int64_t{4}}, {"x", int64_t{5}}}}}},
      {"repeated", list{int64_t{6}, int64_t{7}, record{{"x", int64_t{8}}}}},
      {"null", record{{"", caf::none}, {"x", int64_t{9}}}},
    }}));
}

TEST("event builder can repeat a promoted unflattened prefix") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("k").data_unparsed("1");
  event.field("k.x").data_unparsed("2");
  event.field("k").data_unparsed("3");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"k", list{record{{"", int64_t{1}}, {"x", int64_t{2}}}, int64_t{3}}},
    }}));
}

TEST("event builder preserves raw values in promoted unflattened prefixes") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(
    dh, {.raw = true, .infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("k").data_unparsed("001");
  event.field("k.x").data_unparsed("002");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0],
              (data{record{{"k", record{{"", "001"}, {"x", "002"}}}}}));
}

TEST("event builder unflattens keys that share a prefix into one record") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(dh, {.unflatten_separator = "."});
  auto event = builder.event();
  event.field("id.orig_h").data(std::string_view{"a"});
  event.field("id.orig_p").data(int64_t{1});
  event.field("x").data(int64_t{2});
  event.field("id.resp.h").data(std::string_view{"b"});
  event.field("id.resp.p").data(int64_t{3});
  event.field("id.orig_p").data(int64_t{4});
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0], (data{record{
                           {"id",
                            record{
                              {"orig_h", "a"},
                              {"orig_p", list{int64_t{1}, int64_t{4}}},
                              {"resp",
                               record{
                                 {"h", "b"},
                                 {"p", int64_t{3}},
                               }},
                            }},
                           {"x", int64_t{2}},
                         }}));
}
