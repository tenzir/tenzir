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
