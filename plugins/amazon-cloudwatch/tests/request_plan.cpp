//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Which stages after `from_amazon_cloudwatch` CloudWatch Logs computes, and
// the requests that spell them.

#include "cloudwatch/request_plan.hpp"

#include "cloudwatch/detail/request_payload.hpp"
#include "cloudwatch/operators.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/nova.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <folly/coro/BlockingWait.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::cloudwatch {

namespace {

auto parse(std::string_view text) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(text, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

auto args(std::string mode) -> FromCloudWatchArgs {
  auto result = FromCloudWatchArgs{};
  result.group = {data{std::string{"/group"}}, location::unknown};
  result.mode = {std::move(mode), location::unknown};
  return result;
}

auto replay() -> FromCloudWatchArgs {
  auto result = args("replay");
  result.stream = located<data>{data{std::string{"s"}}, location::unknown};
  return result;
}

auto plan(FromCloudWatchArgs const& args, std::vector<ir::QueryStage> stages)
  -> Option<CloudWatchRequest> {
  return plan_cloudwatch_request(args, stages);
}

/// The filter pattern for `stages` in search mode, or `<none>` if CloudWatch
/// Logs does not compute them.
auto pattern(std::vector<ir::QueryStage> stages, FromCloudWatchArgs const& args
                                                 = cloudwatch::args("search"))
  -> std::string {
  auto request = plan(args, std::move(stages));
  if (not request) {
    return "<none>";
  }
  return request->pattern.value_or("");
}

/// Removes the whitespace outside of the strings of a JSON document.
auto compact(std::string_view json) -> std::string {
  auto result = std::string{};
  auto in_string = false;
  auto escaped = false;
  for (auto c : json) {
    if (in_string) {
      result += c;
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == ' ' or c == '\t' or c == '\n') {
      continue;
    }
    in_string = c == '"';
    result += c;
  }
  return result;
}

/// The JSON of the request that reads the first page.
auto payload(FromCloudWatchArgs const& args, std::vector<ir::QueryStage> stages)
  -> std::string {
  auto request = plan(args, std::move(stages));
  REQUIRE(request);
  switch (from_mode(args)) {
    case FromMode::search:
      return compact(filter_log_events_payload(args, *request, 0, ""));
    case FromMode::replay:
      return compact(get_log_events_payload(args, *request, 0, ""));
    case FromMode::live:
      return compact(live_tail_payload(args, *request));
  }
  TENZIR_UNREACHABLE();
}

auto absorb(FromCloudWatchArgs const& args, std::vector<ir::QueryStage> stages)
  -> std::pair<size_t, std::string> {
  auto result = absorb_cloudwatch_stages(
    args, ir::QueryChain{.stages = std::move(stages), .end = ""});
  return {result.prefix, result.declined};
}

} // namespace

TEST("message terms become a filter pattern") {
  CHECK_EQUAL(pattern({filter(R"("ERROR" in message)")}), R"("ERROR")");
  CHECK_EQUAL(pattern({filter(R"("a b" in message and "c" in message)"),
                       filter(R"(not ("d" in message))")}),
              R"("a b" "c" -"d")");
  CHECK_EQUAL(
    pattern({filter(R"("a" in message or "b" in message or "c" in message)")}),
    R"(?"a" ?"b" ?"c")");
  // Optional terms combine with nothing else, and an excluded term needs an
  // included one.
  CHECK_EQUAL(pattern({filter(R"("a" in message or "b" in message)"),
                       filter(R"("c" in message)")}),
              "<none>");
  CHECK_EQUAL(pattern({filter(R"("a" in message or "b" in message)"),
                       filter(R"("c" in message or "d" in message)")}),
              "<none>");
  CHECK_EQUAL(pattern({filter(R"(not ("d" in message))")}), "<none>");
  CHECK_EQUAL(pattern({filter(R"("a" in message or timestamp > 2024-01-01)")}),
              "<none>");
  // Only plain phrases spell exactly.
  for (auto phrase : {R"(\"x\")", R"(a\\b)", R"(a\tb)", "", "   "}) {
    CHECK_EQUAL(pattern({filter(fmt::format(R"("{}" in message)", phrase))}),
                "<none>");
  }
  // Other fields and shapes stay in Tenzir.
  CHECK_EQUAL(pattern({filter(R"("x" in log_group)")}), "<none>");
  CHECK_EQUAL(pattern({filter(R"(message == "x")")}), "<none>");
  CHECK_EQUAL(pattern({filter(R"(message.starts_with("x"))")}), "<none>");
  // A user's pattern takes no terms.
  auto with_filter = args("search");
  with_filter.filter = located<std::string>{"ERROR", location::unknown};
  CHECK_EQUAL(pattern({filter(R"("x" in message)")}, with_filter), "<none>");
  CHECK_EQUAL(payload(with_filter, {}),
              R"({"logGroupName":"/group","filterPattern":"ERROR"})");
}

TEST("filter patterns stay within 1,024 characters") {
  // With its quotes, a phrase of 1,022 characters spells a pattern of exactly
  // 1,024 characters, and one of 1,023 characters exceeds the limit.
  auto const term = [](size_t length) {
    return filter(fmt::format(R"("{}" in message)", std::string(length, 'x')));
  };
  for (auto mode : {"search", "live"}) {
    auto request = plan(args(mode), {term(1022)});
    REQUIRE(request);
    REQUIRE(request->pattern);
    CHECK_EQUAL(request->pattern->size(), size_t{1024});
    CHECK(not plan(args(mode), {term(1023)}));
    CHECK_EQUAL(absorb(args(mode), {term(1022)}),
                (std::pair<size_t, std::string>{1, ""}));
    CHECK_EQUAL(absorb(args(mode), {term(1023)}),
                (std::pair<size_t, std::string>{
                  0, "the filter pattern would exceed the limit of 1024 "
                     "characters of CloudWatch Logs"}));
  }
  // The stages before the one that exceeds the limit stay absorbed. Here, the
  // second term adds 1 + 2 + 508 characters to the 514 of the first.
  CHECK_EQUAL(absorb(args("search"), {term(512), term(508), limit(3)}),
              (std::pair<size_t, std::string>{1, "the filter pattern would "
                                                 "exceed "
                                                 "the limit of 1024 characters "
                                                 "of CloudWatch Logs"}));
  CHECK_EQUAL(absorb(args("search"), {term(512), term(507), limit(3)}),
              (std::pair<size_t, std::string>{3, ""}));
  // Optional terms count, too: `?"x..." ?"y"` has 8 characters beyond the
  // phrase.
  CHECK(not plan(args("search"),
                 {filter(fmt::format(R"("{}" in message or "y" in message)",
                                     std::string(1017, 'x')))}));
  CHECK(plan(args("search"),
             {filter(fmt::format(R"("{}" in message or "y" in message)",
                                 std::string(1016, 'x')))}));
}

TEST("timestamps narrow the window of a search, inclusively") {
  auto request
    = plan(args("search"), {filter("timestamp >= 2024-01-01T00:00:00.0005Z"),
                            filter("timestamp < 2024-01-02")});
  REQUIRE(request);
  CHECK_EQUAL(request->start_time, Option<int64_t>{1704067200001});
  CHECK_EQUAL(request->end_time, Option<int64_t>{1704153599999});
  CHECK_EQUAL(payload(args("search"), {filter("timestamp >= 2024-01-01"),
                                       filter("timestamp <= 2024-01-02")}),
              R"({"logGroupName":"/group","startTime":1704067200000,)"
              R"("endTime":1704153600000})");
  // Strict comparisons with whole milliseconds move by one.
  CHECK_EQUAL(payload(args("search"), {filter("timestamp > 2024-01-01"),
                                       filter("2024-01-02 > timestamp")}),
              R"({"logGroupName":"/group","startTime":1704067200001,)"
              R"("endTime":1704153599999})");
  // Equality holds for a whole millisecond only.
  CHECK_EQUAL(payload(args("search"), {filter("timestamp == 2024-01-01")}),
              R"({"logGroupName":"/group","startTime":1704067200000,)"
              R"("endTime":1704067200000})");
  request
    = plan(args("search"), {filter("timestamp == 2024-01-01T00:00:00.0005Z")});
  REQUIRE(request);
  CHECK(request->empty);
  // The window only narrows the user's bounds.
  auto bounded = args("search");
  bounded.start
    = located<time>{time{std::chrono::milliseconds{1000}}, location::unknown};
  bounded.end
    = located<time>{time{std::chrono::milliseconds{5000}}, location::unknown};
  CHECK_EQUAL(payload(bounded, {filter("timestamp >= 1970-01-01")}),
              R"({"logGroupName":"/group","startTime":1000,"endTime":5000})");
  CHECK_EQUAL(payload(bounded, {filter("timestamp >= 1970-01-01T00:00:02Z"),
                                filter("timestamp <= 1970-01-01T00:00:09Z")}),
              R"({"logGroupName":"/group","startTime":2000,"endTime":5000})");
  request = plan(bounded, {filter("timestamp > 1970-01-01T00:00:05Z")});
  REQUIRE(request);
  CHECK(request->empty);
  // Times before the epoch match no event.
  request = plan(args("search"), {filter("timestamp < 1969-12-31")});
  REQUIRE(request);
  CHECK(request->empty);
  CHECK_EQUAL(payload(args("search"), {filter("timestamp >= 1969-12-31")}),
              R"({"logGroupName":"/group"})");
  // Other comparisons stay in Tenzir.
  CHECK_EQUAL(pattern({filter("timestamp != 2024-01-01")}), "<none>");
  CHECK_EQUAL(pattern({filter("ingestion_time > 2024-01-01")}), "<none>");
  CHECK_EQUAL(pattern({filter("timestamp > now() - 1h")}), "<none>");
}

TEST("a replay ends its window exclusively") {
  // The plan carries the bounds of the request, which the read copies.
  auto planned = plan(replay(), {filter("timestamp <= 2024-01-02")});
  REQUIRE(planned);
  CHECK(not planned->start_time);
  CHECK_EQUAL(planned->end_time, Option<int64_t>{1704153600001});
  CHECK_EQUAL(payload(replay(), {filter("timestamp >= 2024-01-01"),
                                 filter("timestamp <= 2024-01-02")}),
              R"({"logGroupName":"/group","logStreamName":"s",)"
              R"("startTime":1704067200000,"endTime":1704153600001})");
  CHECK_EQUAL(payload(replay(), {filter("timestamp < 2024-01-02")}),
              R"({"logGroupName":"/group","logStreamName":"s",)"
              R"("endTime":1704153600000})");
  auto bounded = replay();
  bounded.end
    = located<time>{time{std::chrono::milliseconds{5000}}, location::unknown};
  auto request = plan(bounded, {filter("timestamp >= 1970-01-01T00:00:05Z")});
  REQUIRE(request);
  CHECK(request->empty);
  // A replay has no filter pattern and no stream names.
  CHECK(not plan(replay(), {filter(R"("x" in message)")}));
  CHECK(not plan(replay(), {filter(R"(log_stream == "s")")}));
}

TEST("stream names become logStreamNames in a search") {
  CHECK_EQUAL(payload(args("search"), {filter(R"(log_stream == "a")")}),
              R"({"logGroupName":"/group","logStreamNames":["a"]})");
  CHECK_EQUAL(payload(args("search"),
                      {filter(R"(log_stream in ["b", "a", "c", "a"])"),
                       filter(R"(log_stream in ["a", "c", "d"])")}),
              R"({"logGroupName":"/group","logStreamNames":["a","c"]})");
  auto request = plan(args("search"), {filter(R"(log_stream == "a")"),
                                       filter(R"(log_stream == "b")")});
  REQUIRE(request);
  CHECK(request->empty);
  CHECK(not plan(args("search"), {filter(R"(log_stream == "a:b")")}));
  // A stream name may have at most 512 characters.
  auto const longest = std::string(512, 's');
  CHECK_EQUAL(
    payload(args("search"),
            {filter(fmt::format(R"(log_stream == "{}")", longest))}),
    fmt::format(R"({{"logGroupName":"/group","logStreamNames":["{}"]}})",
                longest));
  CHECK(
    not plan(args("search"),
             {filter(fmt::format(R"(log_stream in ["a", "{}s"])", longest))}));
  CHECK(not plan(args("search"), {filter(R"(log_stream != "a")")}));
  auto many = std::string{"log_stream in ["};
  for (auto i = 0; i < 101; ++i) {
    many += fmt::format("{}\"s{}\"", i == 0 ? "" : ",", i);
  }
  many += "]";
  CHECK(not plan(args("search"), {filter(many)}));
  // The user's streams own the parameter.
  auto with_stream = args("search");
  with_stream.stream_prefix = located<std::string>{"pre", location::unknown};
  CHECK(not plan(with_stream, {filter(R"(log_stream == "a")")}));
}

TEST("limits cap the read") {
  CHECK_EQUAL(payload(args("search"), {filter(R"("x" in message)"), limit(5)}),
              R"({"logGroupName":"/group","filterPattern":"\"x\"","limit":5})");
  CHECK_EQUAL(payload(args("search"), {limit(50'000)}),
              R"({"logGroupName":"/group","limit":10000})");
  CHECK_EQUAL(plan(args("search"), {limit(7), limit(3)})->limit,
              Option<uint64_t>{3});
  CHECK(not plan(args("search"), {limit(7), filter(R"("x" in message)")}));
  // The user's `count` limits the events before the stages after it, so
  // only a limit merges.
  auto counted = args("search");
  counted.count = located<uint64_t>{4, location::unknown};
  CHECK_EQUAL(plan(counted, {})->limit, Option<uint64_t>{4});
  CHECK_EQUAL(plan(counted, {limit(2)})->limit, Option<uint64_t>{2});
  CHECK_EQUAL(plan(counted, {limit(9)})->limit, Option<uint64_t>{4});
  CHECK(not plan(counted, {filter(R"("x" in message)")}));
  CHECK(not plan(counted, {filter("timestamp > 2024-01-01")}));
}

TEST("a live read takes over filter patterns only") {
  CHECK_EQUAL(payload(args("live"), {filter(R"("x" in message)")}),
              R"({"logGroupIdentifiers":["/group"],)"
              R"("logEventFilterPattern":"\"x\""})");
  CHECK(not plan(args("live"), {filter("timestamp > 2024-01-01")}));
  CHECK(not plan(args("live"), {filter(R"(log_stream == "a")")}));
  CHECK(not plan(args("live"), {limit(1)}));
  CHECK_EQUAL(absorb(args("live"), {filter(R"("x" in message)"), limit(1)}),
              (std::pair<size_t, std::string>{1, "a live read takes over "
                                                 "filters only"}));
}

TEST("absorption declines what CloudWatch Logs cannot compute") {
  CHECK_EQUAL(absorb(args("search"),
                     {filter(R"("x" in message)"),
                      filter("timestamp > 2024-01-01"), limit(2)}),
              (std::pair<size_t, std::string>{3, ""}));
  CHECK_EQUAL(
    absorb(args("search"),
           {filter(R"("x" in message)"),
            ir::QueryStage{ir::QueryStage::Select{
              .fields = {{.output = {"message"}, .input = {"message"}}}}}}),
    (std::pair<size_t, std::string>{1, "CloudWatch Logs returns a fixed set of "
                                       "fields"}));
  CHECK_EQUAL(absorb(args("search"), {filter(R"(message == "x")")}),
              (std::pair<size_t, std::string>{0, "CloudWatch Logs cannot spell "
                                                 "the stage"}));
  auto counted = args("search");
  counted.count = located<uint64_t>{4, location::unknown};
  CHECK_EQUAL(absorb(counted, {filter(R"("x" in message)")}),
              (std::pair<size_t, std::string>{0, "`count` limits the events "
                                                 "before the stage"}));
}

namespace {

/// Spawns a search that took over `stages`.
auto make_search(std::vector<ir::QueryStage> stages)
  -> Box<Operator<void, nova::Events>> {
  auto const* plugin = plugins::find<OperatorPlugin>("from_amazon_cloudwatch");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_plan);
  REQUIRE(desc.accepts_plan);
  auto any = desc.make_args();
  auto& typed = any.as<FromCloudWatchArgs>();
  typed.group = {data{std::string{"/group"}}, location::unknown};
  typed.mode = {std::string{"search"}, location::unknown};
  (*desc.set_plan)(any, std::move(stages));
  for (auto& spawn : desc.spawns) {
    if (auto* create = try_as<Spawn<void, nova::Events>>(spawn)) {
      return (*create)(std::move(any));
    }
  }
  FAIL("CloudWatch source is not registered");
}

/// Starts `source`, which must complete without creating a client.
auto check_completes(Operator<void, nova::Events>& source) -> void {
  auto dh = collecting_diagnostic_handler{};
  auto ctx = test::NovaOpCtx{dh, test::builtin_registry()};
  folly::coro::blockingWait(source.start(ctx));
  CHECK_EQUAL(source.state(), OperatorState::done);
  CHECK(dh.empty());
}

} // namespace

TEST("an empty window or limit completes without a request") {
  for (auto stage : {limit(0), filter("timestamp < 1969-01-01")}) {
    auto source = make_search({stage});
    check_completes(*source);
  }
}

TEST("a restored read counts its emitted events against the limit") {
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  auto saving = Serde{serializer};
  auto next_token = std::string{"token-2"};
  auto emitted = uint64_t{2};
  auto done = false;
  saving("next_token", next_token);
  saving("emitted", emitted);
  saving("done", done);
  auto source = make_search({limit(2)});
  auto deserializer = caf::binary_deserializer{bytes};
  auto loading = Serde{deserializer};
  source->snapshot(loading);
  CHECK_EQUAL(source->state(), OperatorState::normal);
  check_completes(*source);
}

} // namespace tenzir::plugins::cloudwatch
