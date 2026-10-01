//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/json_parser.hpp"
#include "tenzir/test/test.hpp"

#include <utility>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto make_parser(diagnostic_handler& dh, JsonParser::Settings settings = {})
  -> JsonParser {
  auto parser = JsonParser::make(std::move(settings), dh);
  REQUIRE(parser);
  return std::move(*parser);
}

auto int_field(Events const& events, std::string_view name,
               storage::Index row = 0) -> Int {
  auto field = events.data.field(name);
  REQUIRE(field);
  auto ints = field->data.try_as<Int>();
  REQUIRE(ints);
  return *ints->get(row);
}

} // namespace

TEST("document parsing accepts multiline objects and preserves pending "
     "frames") {
  auto dh = collecting_diagnostic_handler{};
  auto parser = make_parser(dh);
  auto frame = std::string{R"({"id":1})"};
  parser.parse(frame);
  auto document = parser.parse_document("{\n \"id\": 2\n}\n");
  REQUIRE(document);
  CHECK_EQUAL(int_field(*document, "id"), 2);
  CHECK_EQUAL(parser.length(), 1u);
  CHECK(parser.take_ready().empty());
  parser.flush();
  auto ready = parser.take_ready();
  REQUIRE_EQUAL(ready.size(), 1u);
  CHECK_EQUAL(int_field(ready[0], "id"), 1);
  CHECK(std::move(dh).collect().empty());
}

TEST("frame parsing clamps a zero batch size") {
  auto dh = null_diagnostic_handler{};
  auto settings = JsonParser::Settings{};
  settings.batch_size = 0;
  auto parser = make_parser(dh, settings);
  auto frame = std::string{R"({"id":1})"};
  parser.parse(frame);
  CHECK_EQUAL(parser.length(), 0u);
  auto ready = parser.take_ready();
  REQUIRE_EQUAL(ready.size(), 1u);
  CHECK_EQUAL(int_field(ready[0], "id"), 1);
}

TEST("frame parsing batches objects and survives moving the parser") {
  auto dh = collecting_diagnostic_handler{};
  auto settings = JsonParser::Settings{};
  settings.batch_size = 2;
  auto parser = make_parser(dh, settings);
  auto first = std::string{R"({"id":1})"};
  parser.parse(first);
  auto moved = std::move(parser);
  auto second = std::string{R"({"id":2})"};
  moved.parse(second);
  auto ready = moved.take_ready();
  REQUIRE_EQUAL(ready.size(), 1u);
  CHECK_EQUAL(ready[0].length(), 2);
  CHECK_EQUAL(int_field(ready[0], "id", 0), 1);
  CHECK_EQUAL(int_field(ready[0], "id", 1), 2);
  CHECK_EQUAL(moved.length(), 0u);
  auto third = std::string{R"({"id":3})"};
  moved.parse(third);
  moved.flush();
  ready = moved.take_ready();
  REQUIRE_EQUAL(ready.size(), 1u);
  CHECK_EQUAL(int_field(ready[0], "id"), 3);
  CHECK(std::move(dh).collect().empty());
}

TEST("document parsing rejects malformed or trailing input without partial "
     "rows") {
  auto dh = collecting_diagnostic_handler{};
  auto parser = make_parser(dh);
  for (auto source :
       {"", "[]", "null", "{} {}", "{} trailing",
        R"({"id":1,"broken":[true,]})", R"({"id":1,"broken":1e})",
        R"({"id":1,"broken":nul})", R"({"id":1,"broken":truth})",
        R"({"id":1,"broken":01})", R"({"id":1,"broken":18446744073709551616x})",
        R"({"id":1,"broken":})"}) {
    CHECK(not parser.parse_document(source));
    CHECK_EQUAL(parser.length(), 0u);
    CHECK(parser.take_ready().empty());
    auto valid = parser.parse_document(R"({"id":2})");
    REQUIRE(valid);
    CHECK_EQUAL(int_field(*valid, "id"), 2);
  }
  CHECK(not std::move(dh).collect().empty());
}

TEST("decoding policies apply to nested records and lists") {
  auto dh = collecting_diagnostic_handler{};
  auto settings = JsonParser::Settings{};
  settings.decoding.first_duplicate_wins = true;
  settings.decoding.reject_oversized_integers = true;
  auto parser = make_parser(dh, settings);
  auto result = parser.parse_document(
    R"({"id":1,"id":2,"nested":{"x":3,"x":4},"items":[{"x":5,"x":6}]})");
  REQUIRE(result);
  CHECK_EQUAL(int_field(*result, "id"), 1);
  auto nested = result->data.field("nested");
  REQUIRE(nested);
  auto records = nested->data.try_as<Record>();
  REQUIRE(records);
  auto x = records->field("x");
  REQUIRE(x);
  CHECK_EQUAL(*x->data.try_as<Int>()->get(0), 3);
  auto items = result->data.field("items");
  REQUIRE(items);
  auto lists = items->data.try_as<List>();
  REQUIRE(lists);
  auto list = lists->to_primary();
  auto const& elements = as<storage::ListStorage>(list.storage()).values();
  records = elements.try_as<Record>();
  REQUIRE(records);
  x = records->field("x");
  REQUIRE(x);
  CHECK_EQUAL(*x->data.try_as<Int>()->get(0), 5);
  CHECK(not parser.parse_document(R"({"id":1,"x":18446744073709551616})"));
  CHECK(not parser.parse_document(R"({"x":[-9223372036854775809]})"));
  CHECK(not parser.parse_document(R"({"x":1,"x":1e})"));
  CHECK(not parser.parse_document(R"({"x":1,"x":18446744073709551616})"));
}

TEST("selector inference honors the boolean setting") {
  auto dh = collecting_diagnostic_handler{};
  auto settings = JsonParser::Settings{};
  settings.builder.infer_booleans = false;
  settings.builder.policy = EventBuilder::SelectorPolicy{"schema", None{}};
  auto parser = make_parser(dh, settings);
  auto result = parser.parse_document(
    R"({"schema":"missing","text":"true","items":["false"],"ip":"10.0.0.1"})");
  REQUIRE(result);
  auto text = result->data.field("text");
  REQUIRE(text);
  auto strings = text->data.get_alternative<String>();
  REQUIRE(strings);
  CHECK_EQUAL(*strings->data.get(0), "true");
  auto items = result->data.field("items");
  REQUIRE(items);
  auto lists = items->data.get_alternative<List>();
  REQUIRE(lists);
  CHECK(is<RowView<String>>(lists->data.get(0).get(0)));
  auto ip = result->data.field("ip");
  REQUIRE(ip);
  CHECK(ip->data.get_alternative<Ip>());
}

TEST("default decoding collects repeats and retains oversized integer tokens") {
  auto dh = collecting_diagnostic_handler{};
  auto parser = make_parser(dh);
  auto result
    = parser.parse_document(R"({"id":1,"id":2,"big":18446744073709551616})");
  REQUIRE(result);
  auto id = result->data.field("id");
  REQUIRE(id);
  auto lists = id->data.get_alternative<List>();
  REQUIRE(lists);
  CHECK(lists->present.get(0));
  CHECK_EQUAL(lists->data.get(0).length(), 2);
  auto big = result->data.field("big");
  REQUIRE(big);
  auto strings = big->data.try_as<String>();
  REQUIRE(strings);
  CHECK_EQUAL(*strings->get(0), "18446744073709551616");
  CHECK(std::move(dh).collect().empty());
}

TEST("string inference can preserve booleans without disabling extended "
     "types") {
  auto dh = collecting_diagnostic_handler{};
  auto settings = JsonParser::Settings{};
  settings.builder.infer_booleans = false;
  auto parser = make_parser(dh, settings);
  auto result = parser.parse_document(
    R"({"text":"true","bool":true,"nested":{"text":"false"},"items":["true","false",false],"ip":"10.0.0.1","duration":"1h"})");
  REQUIRE(result);
  auto text = result->data.field("text");
  REQUIRE(text);
  CHECK(text->data.try_as<String>());
  auto boolean = result->data.field("bool");
  REQUIRE(boolean);
  CHECK(boolean->data.try_as<Bool>());
  auto nested = result->data.field("nested");
  REQUIRE(nested);
  auto records = nested->data.try_as<Record>();
  REQUIRE(records);
  auto nested_text = records->field("text");
  REQUIRE(nested_text);
  CHECK(nested_text->data.try_as<String>());
  auto ip = result->data.field("ip");
  REQUIRE(ip);
  CHECK(ip->data.try_as<Ip>());
  auto duration = result->data.field("duration");
  REQUIRE(duration);
  CHECK(duration->data.try_as<Duration>());
  auto items = result->data.field("items");
  REQUIRE(items);
  auto lists = items->data.try_as<List>();
  REQUIRE(lists);
  auto const row = lists->get(0);
  CHECK(is<RowView<String>>(row.get(0)));
  CHECK(is<RowView<String>>(row.get(1)));
  CHECK(is<RowView<Bool>>(row.get(2)));
  CHECK(std::move(dh).collect().empty());
}
