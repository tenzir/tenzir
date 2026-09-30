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

TEST("event builder preserves explicit strings after selection") {
  auto dh = null_diagnostic_handler{};
  auto settings = EventBuilder::Settings{};
  settings.policy = EventBuilder::SelectorPolicy{"kind", None{}};
  settings.infer_numbers = true;
  settings.string_fields = {{"x"}, {"nested", "y"}};
  auto builder = make_builder(dh, std::move(settings));
  auto event = builder.event();
  event.field("kind").null();
  auto values = event.field("x").list();
  values.data(std::string_view{"42"});
  values.data(std::string_view{"true"});
  event.field("nested").record().field("y").data(std::string_view{"false"});
  event.field("inferred").data_unparsed("7");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0], (data{record{{"kind", caf::none},
                                      {"x", list{"42", "true"}},
                                      {"nested", record{{"y", "false"}}},
                                      {"inferred", int64_t{7}}}}));
}

TEST("event builder distinguishes literal punctuation in explicit string "
     "paths") {
  auto dh = null_diagnostic_handler{};
  auto settings = EventBuilder::Settings{};
  settings.policy = EventBuilder::SelectorPolicy{"kind", None{}};
  settings.infer_numbers = true;
  settings.string_fields = {{"a.b"}, {"array[]"}};
  auto builder = make_builder(dh, std::move(settings));
  auto event = builder.event();
  event.field("kind").null();
  event.field("a.b").data(std::string_view{"123"});
  event.field("a").record().field("b").data_unparsed("true");
  event.field("array[]").data(std::string_view{"false"});
  event.field("array").list().data_unparsed("true");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0], (data{record{{"kind", caf::none},
                                      {"a.b", "123"},
                                      {"a", record{{"b", true}}},
                                      {"array[]", "false"},
                                      {"array", list{true}}}}));
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

TEST("event builder can retain repeated keys with a schema policy") {
  for (auto merge_structural : {false, true}) {
    for (auto selector : {false, true}) {
      for (auto raw : {false, true}) {
        auto dh = null_diagnostic_handler{};
        auto settings = EventBuilder::Settings{};
        settings.policy
          = selector
              ? EventBuilder::Policy{EventBuilder::SelectorPolicy{"schema",
                                                                  None{}}}
              : EventBuilder::Policy{EventBuilder::SchemaPolicy{"missing"}};
        settings.raw = raw;
        settings.infer_numbers = true;
        settings.unflatten_separator = ".";
        settings.merge_structural = merge_structural;
        auto builder = make_builder(dh, std::move(settings));
        auto event = builder.event();
        event.field("schema").data(std::string_view{"missing"});
        event.field("extra").data_unparsed("1");
        event.field("extra").null();
        event.field("extra").data_unparsed("2");
        auto events = finish(builder);
        REQUIRE_EQUAL(events.size(), 1u);
        auto first = raw ? data{"1"} : data{int64_t{1}};
        auto last = raw ? data{"2"} : data{int64_t{2}};
        auto expected = merge_structural
                          ? data{list{std::move(first), caf::none, last}}
                          : last;
        CHECK_EQUAL(events[0], (data{record{{"schema", "missing"},
                                            {"extra", std::move(expected)}}}));
      }
    }
  }
}

TEST("event builder collects an unflattened scalar prefix with its "
     "descendants") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("k").data_unparsed("1");
  event.field("k.x").data_unparsed("2");
  event.field("nested.k").data_unparsed("4");
  event.field("nested.k.x").data_unparsed("5");
  event.field("repeated").data_unparsed("6");
  event.field("repeated").data_unparsed("7");
  event.field("repeated.x").data_unparsed("8");
  event.field("null").null();
  event.field("null.x").data_unparsed("9");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  // A scalar and a record cannot share a field, so both are kept in a list, in
  // the order in which they arrived, rather than hiding the scalar in a field
  // with an empty name.
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"k", list{int64_t{1}, record{{"x", int64_t{2}}}}},
      {"nested", record{{"k", list{int64_t{4}, record{{"x", int64_t{5}}}}}}},
      {"repeated", list{int64_t{6}, int64_t{7}, record{{"x", int64_t{8}}}}},
      {"null", list{caf::none, record{{"x", int64_t{9}}}}},
    }}));
}

TEST("event builder groups the dotted siblings of a scalar prefix") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  // Siblings continue the record that follows the scalar, like they do when
  // there is no scalar, and other keys in between do not split them.
  event.field("k").data_unparsed("1");
  event.field("k.x").data_unparsed("2");
  event.field("other").data_unparsed("3");
  event.field("k.y").data_unparsed("4");
  event.field("k.z.w").data_unparsed("5");
  // A repeated scalar before the first sibling stays in front of the record.
  event.field("m").data_unparsed("6");
  event.field("m").data_unparsed("7");
  event.field("m.x").data_unparsed("8");
  event.field("m.y").data_unparsed("9");
  // The record may come first, and a scalar in between ends the group.
  event.field("n.x").data_unparsed("10");
  event.field("n").data_unparsed("11");
  event.field("n.y").data_unparsed("12");
  event.field("n.z").data_unparsed("13");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"k", list{int64_t{1}, record{{"x", int64_t{2}},
                                    {"y", int64_t{4}},
                                    {"z", record{{"w", int64_t{5}}}}}}},
      {"other", int64_t{3}},
      {"m", list{int64_t{6}, int64_t{7},
                 record{{"x", int64_t{8}}, {"y", int64_t{9}}}}},
      {"n", list{record{{"x", int64_t{10}}}, int64_t{11},
                 record{{"y", int64_t{12}}, {"z", int64_t{13}}}}},
    }}));
}

TEST("event builder does not merge dotted keys into a list from the input") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  // The input supplied this list, so the record in it is not a prefix that
  // unflattening created.
  auto items = event.field("k").list();
  items.record().field("x").data_unparsed("1");
  event.field("k.y").data_unparsed("2");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"k", list{list{record{{"x", int64_t{1}}}}, record{{"y", int64_t{2}}}}},
    }}));
}

TEST("event builder keeps scalars and unflattened records in arrival order") {
  auto dh = null_diagnostic_handler{};
  auto builder
    = make_builder(dh, {.infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("before").data_unparsed("1");
  event.field("before.x").data_unparsed("2");
  event.field("after.x").data_unparsed("3");
  event.field("after").data_unparsed("4");
  event.field("between").data_unparsed("5");
  event.field("between.x").data_unparsed("6");
  event.field("between").data_unparsed("7");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(
    events[0],
    (data{record{
      {"before", list{int64_t{1}, record{{"x", int64_t{2}}}}},
      {"after", list{record{{"x", int64_t{3}}}, int64_t{4}}},
      {"between", list{int64_t{5}, record{{"x", int64_t{6}}}, int64_t{7}}},
    }}));
}

TEST("event builder preserves raw values in an unflattened scalar prefix") {
  auto dh = null_diagnostic_handler{};
  auto builder = make_builder(
    dh, {.raw = true, .infer_numbers = true, .unflatten_separator = "."});
  auto event = builder.event();
  event.field("k").data_unparsed("001");
  event.field("k.x").data_unparsed("002");
  auto events = finish(builder);
  REQUIRE_EQUAL(events.size(), 1u);
  CHECK_EQUAL(events[0],
              (data{record{{"k", list{"001", record{{"x", "002"}}}}}}));
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
