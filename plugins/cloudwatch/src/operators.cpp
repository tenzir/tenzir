//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/operators.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/eval_as.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>

#include <aws/core/utils/Outcome.h>
#include <aws/logs/model/FilterLogEventsRequest.h>
#include <aws/logs/model/FilteredLogEvent.h>
#include <aws/logs/model/GetLogEventsRequest.h>
#include <aws/logs/model/InputLogEvent.h>
#include <aws/logs/model/OutputLogEvent.h>
#include <aws/logs/model/PutLogEventsRequest.h>
#include <aws/logs/model/StartLiveTailRequest.h>
#include <folly/coro/BlockingWait.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <ranges>
#include <simdjson.h>

namespace tenzir::plugins::cloudwatch {

namespace {

constexpr auto max_put_events = size_t{10'000};
constexpr auto max_put_request_bytes = size_t{1'048'576};
constexpr auto put_event_overhead = size_t{26};
constexpr auto max_put_event_bytes = max_put_request_bytes - put_event_overhead;
constexpr auto max_put_batch_span = std::chrono::hours{24};
constexpr auto max_future_skew = std::chrono::hours{2};
constexpr auto max_past_skew = std::chrono::hours{24 * 14};

auto to_mode(std::string_view value) -> FromMode {
  if (value == "filter") {
    return FromMode::filter;
  }
  if (value == "get") {
    return FromMode::get;
  }
  return FromMode::live;
}

auto to_method(std::string_view value) -> ToMethod {
  if (value == "hlc") {
    return ToMethod::hlc;
  }
  return ToMethod::put_log_events;
}

auto epoch_ms(time value) -> int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           value.time_since_epoch())
    .count();
}

auto from_epoch_ms(int64_t value) -> time {
  return time{std::chrono::milliseconds{value}};
}

auto now_time() -> time {
  return time{std::chrono::duration_cast<time::duration>(
    std::chrono::system_clock::now().time_since_epoch())};
}

auto aws_string(Aws::String const& value) -> std::string {
  return std::string{std::string_view{value.data(), value.size()}};
}

auto make_aws_vector(std::vector<std::string> const& values)
  -> Aws::Vector<Aws::String> {
  auto result = Aws::Vector<Aws::String>{};
  result.reserve(values.size());
  for (auto const& value : values) {
    result.emplace_back(value.data(), value.size());
  }
  return result;
}

auto strings_from_data(located<data> const& value) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  auto const* values = try_as<list>(&value.inner);
  TENZIR_ASSERT(values);
  result.reserve(values->size());
  for (auto const& item : *values) {
    auto const* str = try_as<std::string>(&item);
    TENZIR_ASSERT(str);
    result.push_back(*str);
  }
  return result;
}

auto validate_string_list(located<data> const& value, std::string_view name,
                          diagnostic_handler& dh) -> bool {
  auto const* values = try_as<list>(&value.inner);
  if (not values) {
    diagnostic::error("`{}` must be a list of strings", name)
      .primary(value.source)
      .emit(dh);
    return false;
  }
  for (auto const& item : *values) {
    if (not try_as<std::string>(&item)) {
      diagnostic::error("`{}` must be a list of strings", name)
        .primary(value.source)
        .emit(dh);
      return false;
    }
  }
  return true;
}

auto selected_log_streams(FromCloudWatchArgs const& args)
  -> std::vector<std::string> {
  if (args.log_streams) {
    return strings_from_data(*args.log_streams);
  }
  if (args.log_stream) {
    return {args.log_stream->inner};
  }
  return {};
}

struct CloudWatchEvent {
  time timestamp;
  time ingestion_time;
  std::string log_group;
  std::string log_stream;
  std::string message;
  Option<std::string> event_id;
};

struct SourcePage {
  std::vector<CloudWatchEvent> events;
  std::string next_token;
  std::string error;
  bool done = false;
};

auto aws_error(
  std::string_view operation,
  Aws::Client::AWSError<Aws::CloudWatchLogs::CloudWatchLogsErrors> const& error)
  -> std::string {
  return fmt::format("{} failed: {} ({})", operation, error.GetMessage(),
                     error.GetExceptionName());
}

auto build_slice(std::vector<CloudWatchEvent> const& events,
                 diagnostic_handler& dh) -> std::vector<table_slice> {
  auto opts = multi_series_builder::options{};
  opts.settings.ordered = true;
  opts.settings.raw = true;
  opts.settings.default_schema_name = "tenzir.cloudwatch";
  auto msb = multi_series_builder{std::move(opts), dh};
  for (auto const& event : events) {
    auto record = msb.record();
    record.field("timestamp").data(event.timestamp);
    record.field("ingestion_time").data(event.ingestion_time);
    record.field("log_group").data(event.log_group);
    record.field("log_stream").data(event.log_stream);
    record.field("message").data(event.message);
    if (event.event_id) {
      record.field("event_id").data(*event.event_id);
    }
  }
  return msb.finalize_as_table_slice();
}

auto append_filter_options(
  Aws::CloudWatchLogs::Model::FilterLogEventsRequest& request,
  FromCloudWatchArgs const& args) -> void {
  request.SetLogGroupName(args.log_group.inner);
  if (args.filter) {
    request.SetFilterPattern(args.filter->inner);
  }
  if (args.start) {
    request.SetStartTime(epoch_ms(args.start->inner));
  }
  if (args.end) {
    request.SetEndTime(epoch_ms(args.end->inner));
  }
  if (args.limit) {
    request.SetLimit(detail::narrow<int>(args.limit->inner));
  }
  auto log_streams = selected_log_streams(args);
  if (not log_streams.empty()) {
    request.SetLogStreamNames(make_aws_vector(log_streams));
  }
  if (args.log_stream_prefix) {
    request.SetLogStreamNamePrefix(args.log_stream_prefix->inner);
  }
  if (args.unmask) {
    request.SetUnmask(args.unmask->inner);
  }
}

auto append_get_options(Aws::CloudWatchLogs::Model::GetLogEventsRequest& request,
                        FromCloudWatchArgs const& args) -> void {
  request.SetLogGroupName(args.log_group.inner);
  request.SetLogStreamName(args.log_stream->inner);
  if (args.start) {
    request.SetStartTime(epoch_ms(args.start->inner));
  }
  if (args.end) {
    request.SetEndTime(epoch_ms(args.end->inner));
  }
  if (args.limit) {
    request.SetLimit(detail::narrow<int>(args.limit->inner));
  }
  if (args.start_from_head) {
    request.SetStartFromHead(args.start_from_head->inner);
  }
  if (args.unmask) {
    request.SetUnmask(args.unmask->inner);
  }
}

auto filter_page(Aws::CloudWatchLogs::CloudWatchLogsClient& client,
                 FromCloudWatchArgs args, std::string next_token)
  -> SourcePage {
  auto request = Aws::CloudWatchLogs::Model::FilterLogEventsRequest{};
  append_filter_options(request, args);
  if (not next_token.empty()) {
    request.SetNextToken(next_token);
  }
  auto outcome = client.FilterLogEvents(request);
  if (not outcome.IsSuccess()) {
    return SourcePage{
      .error = aws_error("FilterLogEvents", outcome.GetError()),
      .done = true,
    };
  }
  auto const& result = outcome.GetResult();
  auto page = SourcePage{};
  page.next_token = aws_string(result.GetNextToken());
  page.done = page.next_token.empty();
  page.events.reserve(result.GetEvents().size());
  for (auto const& event : result.GetEvents()) {
    page.events.push_back(CloudWatchEvent{
      .timestamp = from_epoch_ms(event.GetTimestamp()),
      .ingestion_time = from_epoch_ms(event.GetIngestionTime()),
      .log_group = args.log_group.inner,
      .log_stream = aws_string(event.GetLogStreamName()),
      .message = aws_string(event.GetMessage()),
      .event_id = aws_string(event.GetEventId()),
    });
  }
  return page;
}

auto get_page(Aws::CloudWatchLogs::CloudWatchLogsClient& client,
              FromCloudWatchArgs args, std::string next_token) -> SourcePage {
  auto request = Aws::CloudWatchLogs::Model::GetLogEventsRequest{};
  append_get_options(request, args);
  if (not next_token.empty()) {
    request.SetNextToken(next_token);
  }
  auto outcome = client.GetLogEvents(request);
  if (not outcome.IsSuccess()) {
    return SourcePage{
      .error = aws_error("GetLogEvents", outcome.GetError()),
      .done = true,
    };
  }
  auto const& result = outcome.GetResult();
  auto page = SourcePage{};
  page.next_token = aws_string(result.GetNextForwardToken());
  page.done = page.next_token.empty() or page.next_token == next_token;
  page.events.reserve(result.GetEvents().size());
  for (auto const& event : result.GetEvents()) {
    page.events.push_back(CloudWatchEvent{
      .timestamp = from_epoch_ms(event.GetTimestamp()),
      .ingestion_time = from_epoch_ms(event.GetIngestionTime()),
      .log_group = args.log_group.inner,
      .log_stream = args.log_stream->inner,
      .message = aws_string(event.GetMessage()),
      .event_id = std::nullopt,
    });
  }
  return page;
}

auto live_tail_request(FromCloudWatchArgs const& args)
  -> Aws::CloudWatchLogs::Model::StartLiveTailRequest {
  auto request = Aws::CloudWatchLogs::Model::StartLiveTailRequest{};
  auto groups = std::vector<std::string>{};
  if (args.log_group_identifiers) {
    groups = strings_from_data(*args.log_group_identifiers);
  }
  if (groups.empty()) {
    groups.push_back(args.log_group.inner);
  }
  request.SetLogGroupIdentifiers(make_aws_vector(groups));
  if (args.filter) {
    request.SetLogEventFilterPattern(args.filter->inner);
  }
  auto log_streams = selected_log_streams(args);
  if (not log_streams.empty()) {
    request.SetLogStreamNames(make_aws_vector(log_streams));
  }
  if (args.log_stream_prefix) {
    request.SetLogStreamNamePrefixes(
      Aws::Vector<Aws::String>{Aws::String{args.log_stream_prefix->inner}});
  }
  return request;
}

auto live_tail_page(
  Aws::CloudWatchLogs::Model::LiveTailSessionUpdate const& update)
  -> SourcePage {
  auto page = SourcePage{};
  for (auto const& event : update.GetSessionResults()) {
    page.events.push_back(CloudWatchEvent{
      .timestamp = from_epoch_ms(event.GetTimestamp()),
      .ingestion_time = from_epoch_ms(event.GetIngestionTime()),
      .log_group = aws_string(event.GetLogGroupIdentifier()),
      .log_stream = aws_string(event.GetLogStreamName()),
      .message = aws_string(event.GetMessage()),
      .event_id = std::nullopt,
    });
  }
  return page;
}

auto append_json_event(std::string& out, ToCloudWatch::Event const& event)
  -> void {
  out += R"({"timestamp":)";
  out += std::to_string(epoch_ms(event.timestamp));
  out += R"(,"message":)";
  out += detail::json_escape(event.message);
  out += "}";
}

auto json_event(ToCloudWatch::Event const& event) -> std::string {
  auto result = std::string{};
  append_json_event(result, event);
  return result;
}

auto put_log_events_body(
  std::string_view group, std::string_view stream,
  Aws::Vector<Aws::CloudWatchLogs::Model::InputLogEvent> const& events)
  -> std::string {
  auto out = std::string{"{\"logGroupName\":"};
  out += detail::json_escape(group);
  out += ",\"logStreamName\":";
  out += detail::json_escape(stream);
  out += ",\"logEvents\":[";
  auto first = true;
  for (auto const& event : events) {
    if (not first) {
      out += ",";
    }
    first = false;
    out += "{\"timestamp\":";
    out += std::to_string(event.GetTimestamp());
    out += ",\"message\":";
    out += detail::json_escape(aws_string(event.GetMessage()));
    out += "}";
  }
  out += "]}";
  return out;
}

auto warn_rejected_events(
  Aws::CloudWatchLogs::Model::RejectedLogEventsInfo const& rejected,
  location primary, diagnostic_handler& dh) -> void {
  if (not rejected.TooNewLogEventStartIndexHasBeenSet()
      and not rejected.TooOldLogEventEndIndexHasBeenSet()
      and not rejected.ExpiredLogEventEndIndexHasBeenSet()) {
    return;
  }
  if (rejected.TooNewLogEventStartIndexHasBeenSet()) {
    diagnostic::warning("CloudWatch rejected too-new log events")
      .primary(primary)
      .note("events start at batch index {}",
            rejected.GetTooNewLogEventStartIndex())
      .emit(dh);
  }
  if (rejected.TooOldLogEventEndIndexHasBeenSet()) {
    diagnostic::warning("CloudWatch rejected too-old log events")
      .primary(primary)
      .note("events end before batch index {}",
            rejected.GetTooOldLogEventEndIndex())
      .emit(dh);
  }
  if (rejected.ExpiredLogEventEndIndexHasBeenSet()) {
    diagnostic::warning("CloudWatch rejected expired log events")
      .primary(primary)
      .note("events end before batch index {}",
            rejected.GetExpiredLogEventEndIndex())
      .emit(dh);
  }
}

auto filter_log_events_body(FromCloudWatchArgs const& args,
                            std::string_view next_token) -> std::string {
  auto out = std::string{"{\"logGroupName\":"};
  out += detail::json_escape(args.log_group.inner);
  if (args.filter) {
    out += ",\"filterPattern\":";
    out += detail::json_escape(args.filter->inner);
  }
  if (args.start) {
    out += ",\"startTime\":";
    out += std::to_string(epoch_ms(args.start->inner));
  }
  if (args.end) {
    out += ",\"endTime\":";
    out += std::to_string(epoch_ms(args.end->inner));
  }
  if (args.limit) {
    out += ",\"limit\":";
    out += std::to_string(args.limit->inner);
  }
  if (not next_token.empty()) {
    out += ",\"nextToken\":";
    out += detail::json_escape(next_token);
  }
  auto log_streams = selected_log_streams(args);
  if (not log_streams.empty()) {
    out += ",\"logStreamNames\":[";
    auto first = true;
    for (auto const& stream : log_streams) {
      if (not first) {
        out += ",";
      }
      first = false;
      out += detail::json_escape(stream);
    }
    out += "]";
  }
  if (args.log_stream_prefix) {
    out += ",\"logStreamNamePrefix\":";
    out += detail::json_escape(args.log_stream_prefix->inner);
  }
  if (args.unmask) {
    out += ",\"unmask\":";
    out += args.unmask->inner ? "true" : "false";
  }
  out += "}";
  return out;
}

auto get_log_events_body(FromCloudWatchArgs const& args,
                         std::string_view next_token) -> std::string {
  auto out = std::string{"{\"logGroupName\":"};
  out += detail::json_escape(args.log_group.inner);
  out += ",\"logStreamName\":";
  out += detail::json_escape(args.log_stream->inner);
  if (args.start) {
    out += ",\"startTime\":";
    out += std::to_string(epoch_ms(args.start->inner));
  }
  if (args.end) {
    out += ",\"endTime\":";
    out += std::to_string(epoch_ms(args.end->inner));
  }
  if (args.limit) {
    out += ",\"limit\":";
    out += std::to_string(args.limit->inner);
  }
  if (not next_token.empty()) {
    out += ",\"nextToken\":";
    out += detail::json_escape(next_token);
  }
  if (args.start_from_head) {
    out += ",\"startFromHead\":";
    out += args.start_from_head->inner ? "true" : "false";
  }
  if (args.unmask) {
    out += ",\"unmask\":";
    out += args.unmask->inner ? "true" : "false";
  }
  out += "}";
  return out;
}

auto string_from_json(simdjson::dom::element obj, std::string_view key)
  -> std::string {
  auto value = obj[key];
  if (value.error()) {
    return {};
  }
  auto str = std::string_view{};
  if (value.get(str) != simdjson::SUCCESS) {
    return {};
  }
  return std::string{str};
}

auto int_from_json(simdjson::dom::element obj, std::string_view key)
  -> int64_t {
  auto value = obj[key];
  if (value.error()) {
    return {};
  }
  auto result = int64_t{};
  if (value.get(result) != simdjson::SUCCESS) {
    return {};
  }
  return result;
}

auto parse_filter_log_events_response(std::string const& body,
                                      std::string const& log_group)
  -> SourcePage {
  auto parser = simdjson::dom::parser{};
  auto doc = parser.parse(body);
  if (doc.error()) {
    return SourcePage{.error
                      = fmt::format("failed to parse CloudWatch Logs "
                                    "response: {}",
                                    simdjson::error_message(doc.error())),
                      .done = true};
  }
  auto page = SourcePage{};
  page.next_token = string_from_json(doc.value(), "nextToken");
  page.done = page.next_token.empty();
  auto events = doc["events"];
  if (events.error()) {
    return page;
  }
  for (auto event : events) {
    auto message = string_from_json(event, "message");
    page.events.push_back(CloudWatchEvent{
      .timestamp = from_epoch_ms(int_from_json(event, "timestamp")),
      .ingestion_time = from_epoch_ms(int_from_json(event, "ingestionTime")),
      .log_group = log_group,
      .log_stream = string_from_json(event, "logStreamName"),
      .message = std::move(message),
      .event_id = string_from_json(event, "eventId"),
    });
  }
  return page;
}

auto parse_get_log_events_response(std::string const& body,
                                   std::string const& log_group,
                                   std::string const& log_stream,
                                   std::string const& previous_token)
  -> SourcePage {
  auto parser = simdjson::dom::parser{};
  auto doc = parser.parse(body);
  if (doc.error()) {
    return SourcePage{.error
                      = fmt::format("failed to parse CloudWatch Logs "
                                    "response: {}",
                                    simdjson::error_message(doc.error())),
                      .done = true};
  }
  auto page = SourcePage{};
  page.next_token = string_from_json(doc.value(), "nextForwardToken");
  page.done = page.next_token.empty() or page.next_token == previous_token;
  auto events = doc["events"];
  if (events.error()) {
    return page;
  }
  for (auto event : events) {
    page.events.push_back(CloudWatchEvent{
      .timestamp = from_epoch_ms(int_from_json(event, "timestamp")),
      .ingestion_time = from_epoch_ms(int_from_json(event, "ingestionTime")),
      .log_group = log_group,
      .log_stream = log_stream,
      .message = string_from_json(event, "message"),
      .event_id = std::nullopt,
    });
  }
  return page;
}

} // namespace

auto default_to_cloudwatch_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

FromCloudWatch::FromCloudWatch(FromCloudWatchArgs args)
  : args_{std::move(args)} {
}

auto FromCloudWatch::start(OpCtx& ctx) -> Task<void> {
  mode_ = to_mode(args_.mode.inner);
  if (args_.log_group_identifiers
      and not validate_string_list(*args_.log_group_identifiers,
                                   "log_group_identifiers", ctx.dh())) {
    done_ = true;
    co_return;
  }
  if (args_.log_streams
      and not validate_string_list(*args_.log_streams, "log_streams",
                                   ctx.dh())) {
    done_ = true;
    co_return;
  }
  if (mode_ == FromMode::live and args_.log_stream_prefix
      and args_.log_group_identifiers
      and strings_from_data(*args_.log_group_identifiers).size() > size_t{1}) {
    diagnostic::error("`log_stream_prefix` requires exactly one log group")
      .primary(args_.log_stream_prefix->source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  if (args_.limit
      and args_.limit->inner > uint64_t{std::numeric_limits<int>::max()}) {
    diagnostic::error("limit must not exceed {}",
                      std::numeric_limits<int>::max())
      .primary(args_.limit->source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  if (mode_ != FromMode::get and args_.log_stream and args_.log_streams) {
    diagnostic::error("`log_stream` and `log_streams` are mutually exclusive")
      .primary(args_.log_streams->source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  if (mode_ != FromMode::get and args_.log_streams
      and args_.log_stream_prefix) {
    diagnostic::error("`log_streams` and `log_stream_prefix` are mutually "
                      "exclusive")
      .primary(args_.log_stream_prefix->source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  if (mode_ != FromMode::get and args_.log_stream and args_.log_stream_prefix) {
    diagnostic::error("`log_stream` and `log_stream_prefix` are mutually "
                      "exclusive")
      .primary(args_.log_stream_prefix->source)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  auto client = co_await make_cloudwatch_client(args_.aws_iam, args_.aws_region,
                                                args_.log_group.source, ctx);
  if (not client) {
    done_ = true;
    co_return;
  }
  client_ = std::move(client->logs);
  auto endpoint = detail::getenv("AWS_ENDPOINT_URL_LOGS");
  if (not endpoint) {
    endpoint = detail::getenv("AWS_ENDPOINT_URL");
  }
  if (endpoint) {
    try {
      auto config = HttpPoolConfig{};
      config.tls = endpoint->starts_with("https://");
      http_pool_
        = HttpPool::make(ctx.io_executor(), *endpoint, std::move(config));
      use_local_http_read_ = true;
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.operator_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
  }
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_cloudwatch"},
                       MetricsDirection::read, MetricsVisibility::external_);
  if (mode_ == FromMode::live) {
    auto [sender, receiver] = channel<Any>(16);
    live_rx_ = std::move(receiver);
    auto sender_ptr = std::make_shared<Sender<Any>>(std::move(sender));
    auto client = *client_;
    auto args = args_;
    ctx.spawn_task([client = std::move(client), args = std::move(args),
                    sender = std::move(sender_ptr)]() mutable -> Task<void> {
      auto outcome = co_await spawn_blocking([client = std::move(client),
                                              args = std::move(args),
                                              sender
                                              = std::move(sender)]() mutable {
        auto request = live_tail_request(args);
        auto handler = Aws::CloudWatchLogs::Model::StartLiveTailHandler{};
        handler.SetLiveTailSessionUpdateCallback(
          [sender](Aws::CloudWatchLogs::Model::LiveTailSessionUpdate const&
                     update) mutable {
            folly::coro::blockingWait(
              sender->send(Any{live_tail_page(update)}));
          });
        handler.SetOnErrorCallback(
          [sender](
            Aws::Client::AWSError<
              Aws::CloudWatchLogs::CloudWatchLogsErrors> const& error) mutable {
            folly::coro::blockingWait(sender->send(Any{SourcePage{
              .error = aws_error("StartLiveTail", error),
              .done = true,
            }}));
          });
        request.SetEventStreamHandler(handler);
        return client->StartLiveTail(request);
      });
      if (not outcome.IsSuccess()) {
        folly::coro::blockingWait(sender->send(Any{SourcePage{
          .error = aws_error("StartLiveTail", outcome.GetError()),
          .done = true,
        }}));
      }
    });
  }
}

auto FromCloudWatch::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  if (done_) {
    co_await wait_forever();
    TENZIR_UNREACHABLE();
  }
  if (mode_ == FromMode::live) {
    auto result = co_await live_rx_->recv();
    if (not result) {
      co_return SourcePage{.done = true};
    }
    co_return std::move(*result);
  }
  auto client = client_;
  auto args = args_;
  auto token = next_token_;
  auto mode = mode_;
  if (use_local_http_read_
      and (mode == FromMode::get or (mode == FromMode::filter and args_.filter))
      and http_pool_) {
    auto headers = std::map<std::string, std::string>{
      {"content-type", "application/x-amz-json-1.1"},
      {"x-amz-target", mode == FromMode::filter
                         ? "Logs_20140328.FilterLogEvents"
                         : "Logs_20140328.GetLogEvents"},
    };
    auto body = mode == FromMode::filter ? filter_log_events_body(args_, token)
                                         : get_log_events_body(args_, token);
    auto response
      = co_await (*http_pool_)->post(std::move(body), std::move(headers));
    if (response.is_ok()) {
      auto http_response = std::move(response).unwrap();
      if (http_response.is_status_success()) {
        co_return mode == FromMode::filter
          ? parse_filter_log_events_response(http_response.body,
                                             args_.log_group.inner)
          : parse_get_log_events_response(http_response.body,
                                          args_.log_group.inner,
                                          args_.log_stream->inner, token);
      }
    }
  }
  auto page = co_await spawn_blocking(
    [client = std::move(client), args = std::move(args),
     token = std::move(token), mode]() mutable -> SourcePage {
      if (mode == FromMode::filter) {
        return filter_page(**client, std::move(args), std::move(token));
      }
      return get_page(**client, std::move(args), std::move(token));
    });
  if (not page.error.empty() and use_local_http_read_ and http_pool_) {
    auto headers = std::map<std::string, std::string>{
      {"content-type", "application/x-amz-json-1.1"},
      {"x-amz-target", mode == FromMode::filter
                         ? "Logs_20140328.FilterLogEvents"
                         : "Logs_20140328.GetLogEvents"},
    };
    auto body = mode == FromMode::filter ? filter_log_events_body(args_, token)
                                         : get_log_events_body(args_, token);
    auto response
      = co_await (*http_pool_)->post(std::move(body), std::move(headers));
    if (response.is_ok()) {
      auto http_response = std::move(response).unwrap();
      if (http_response.is_status_success()) {
        page
          = mode == FromMode::filter
              ? parse_filter_log_events_response(http_response.body,
                                                 args_.log_group.inner)
              : parse_get_log_events_response(http_response.body,
                                              args_.log_group.inner,
                                              args_.log_stream->inner, token);
      }
    }
  }
  co_return std::move(page);
}

auto FromCloudWatch::process_task(Any result, Push<table_slice>& push,
                                  OpCtx& ctx) -> Task<void> {
  auto page = std::move(result).as<SourcePage>();
  if (not page.error.empty()) {
    diagnostic::error("{}", page.error)
      .primary(args_.operator_location)
      .emit(ctx);
    done_ = true;
    co_return;
  }
  next_token_ = std::move(page.next_token);
  done_ = page.done;
  if (args_.limit) {
    auto remaining = args_.limit->inner - emitted_;
    if (remaining == 0) {
      done_ = true;
      co_return;
    }
    if (page.events.size() > remaining) {
      page.events.resize(detail::narrow<size_t>(remaining));
      done_ = true;
    }
  }
  for (auto const& event : page.events) {
    bytes_read_counter_.add(event.message.size());
  }
  for (auto&& slice : build_slice(page.events, ctx.dh())) {
    emitted_ += slice.rows();
    co_await push(std::move(slice));
  }
  if (args_.limit and emitted_ >= args_.limit->inner) {
    done_ = true;
  }
}

auto FromCloudWatch::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

ToCloudWatch::ToCloudWatch(ToCloudWatchArgs args)
  : args_{std::move(args)}, request_slots_{1} {
}

auto ToCloudWatch::start(OpCtx& ctx) -> Task<void> {
  method_ = to_method(args_.method.inner);
  batch_size_ = args_.batch_size ? args_.batch_size->inner : uint64_t{1000};
  batch_timeout_ = args_.batch_timeout ? args_.batch_timeout->inner
                                       : std::chrono::seconds{1};
  parallel_ = args_.parallel ? args_.parallel->inner : uint64_t{1};
  request_slots_ = Semaphore{detail::narrow<size_t>(parallel_)};
  bytes_write_counter_
    = ctx.make_counter(MetricsLabel{"operator", "to_cloudwatch"},
                       MetricsDirection::write, MetricsVisibility::external_);
  if (method_ == ToMethod::hlc) {
    auto requests = std::vector<secret_request>{
      make_secret_request("token", *args_.token, token_, ctx.dh()),
    };
    if (auto result = co_await ctx.resolve_secrets(std::move(requests));
        result.is_error()) {
      co_return;
    }
    auto url = args_.endpoint
                 ? args_.endpoint->inner
                 : std::string{"https://logs.cloudwatch.amazonaws.com/logs"};
    try {
      auto config = HttpPoolConfig{};
      config.tls = url.starts_with("https://");
      http_pool_ = HttpPool::make(ctx.io_executor(), url, std::move(config));
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.operator_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    co_return;
  }
  auto client = co_await make_cloudwatch_client(args_.aws_iam, args_.aws_region,
                                                args_.log_group.source, ctx);
  if (client) {
    client_ = std::move(client->logs);
  } else {
    done_ = true;
  }
  auto endpoint = detail::getenv("AWS_ENDPOINT_URL_LOGS");
  if (not endpoint) {
    endpoint = detail::getenv("AWS_ENDPOINT_URL");
  }
  if (endpoint) {
    try {
      auto config = HttpPoolConfig{};
      config.tls = endpoint->starts_with("https://");
      http_pool_
        = HttpPool::make(ctx.io_executor(), *endpoint, std::move(config));
      use_local_http_put_ = true;
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.operator_location)
        .emit(ctx);
      done_ = true;
      co_return;
    }
  }
}

auto ToCloudWatch::process(table_slice input, OpCtx& ctx) -> Task<void> {
  if (done_) {
    co_return;
  }
  if (input.rows() == 0) {
    co_return;
  }
  auto& dh = ctx.dh();
  auto timestamps
    = detail::eval_as<time_type>("timestamp", args_.timestamp, input, dh, [] {
        return now_time();
      });
  for (auto const& messages : eval(args_.message, input, dh)) {
    auto append = [&](auto const& array) -> Task<void> {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        auto t = now_time();
        if (auto next = timestamps.next(); next and *next) {
          t = **next;
        }
        if (array.IsNull(i)) {
          diagnostic::warning("expected `string` or `blob`, got `null`")
            .primary(args_.message)
            .note("event is skipped")
            .emit(dh);
          continue;
        }
        auto bytes = as_bytes(array.Value(i));
        auto message = std::string{reinterpret_cast<char const*>(bytes.data()),
                                   bytes.size()};
        if (message.size() > max_put_event_bytes) {
          diagnostic::warning(
            "CloudWatch log event exceeds maximum payload size")
            .primary(args_.message)
            .note("event is skipped")
            .emit(dh);
          continue;
        }
        if (batch_started_at_
            and now_time() - *batch_started_at_ >= batch_timeout_) {
          co_await flush(ctx);
        }
        if (batch_.empty()) {
          batch_started_at_ = now_time();
        }
        batch_.push_back(Event{.timestamp = t, .message = std::move(message)});
        if (batch_.size() >= batch_size_) {
          co_await flush(ctx);
        }
      }
    };
    if (auto strings = messages.template as<string_type>()) {
      co_await append(*strings->array);
      continue;
    }
    if (auto blobs = messages.template as<blob_type>()) {
      co_await append(*blobs->array);
      continue;
    }
    diagnostic::warning("expected `string` or `blob`, got `{}`",
                        messages.type.kind())
      .primary(args_.message)
      .note("events are skipped")
      .emit(dh);
  }
}

auto ToCloudWatch::flush(OpCtx& ctx) -> Task<void> {
  if (batch_.empty()) {
    co_return;
  }
  auto events = std::exchange(batch_, {});
  batch_started_at_ = None{};
  auto permit = co_await request_slots_.acquire();
  auto* dh = &ctx.dh();
  if (method_ == ToMethod::hlc) {
    ctx.spawn_task([this, events = std::move(events),
                    permit = std::move(permit), dh]() mutable -> Task<void> {
      co_await send_hlc_batch(std::move(events), *dh);
      permit.release();
    });
    co_return;
  }
  ctx.spawn_task([this, events = std::move(events), permit = std::move(permit),
                  dh]() mutable -> Task<void> {
    co_await send_put_batch(std::move(events), *dh);
    permit.release();
  });
}

auto ToCloudWatch::send_put_batch(std::vector<Event> events,
                                  diagnostic_handler& dh) -> Task<void> {
  if (not client_) {
    co_return;
  }
  std::ranges::sort(events, {}, &Event::timestamp);
  auto now = std::chrono::system_clock::now();
  auto request_events
    = Aws::Vector<Aws::CloudWatchLogs::Model::InputLogEvent>{};
  auto request_bytes = size_t{};
  auto first_timestamp = Option<time>{};
  for (auto i = size_t{}; i < events.size(); ++i) {
    auto& event = events[i];
    if (event.timestamp > now + max_future_skew
        or event.timestamp < now - max_past_skew) {
      diagnostic::warning("CloudWatch log event timestamp is outside the "
                          "accepted time window")
        .primary(args_.operator_location)
        .note("event is skipped")
        .emit(dh);
      continue;
    }
    auto event_bytes = event.message.size() + put_event_overhead;
    auto exceeds_batch_span
      = first_timestamp
        and event.timestamp - *first_timestamp > max_put_batch_span;
    if (request_events.size() >= max_put_events
        or request_bytes + event_bytes > max_put_request_bytes
        or exceeds_batch_span) {
      auto rest = std::vector<Event>{};
      rest.reserve(events.size() - i);
      for (auto& pending : events | std::views::drop(i)) {
        rest.push_back(std::move(pending));
      }
      co_await send_put_batch(std::move(rest), dh);
      break;
    }
    if (not first_timestamp) {
      first_timestamp = event.timestamp;
    }
    auto input = Aws::CloudWatchLogs::Model::InputLogEvent{};
    input.SetTimestamp(epoch_ms(event.timestamp));
    input.SetMessage(Aws::String{event.message.data(), event.message.size()});
    request_events.push_back(std::move(input));
    request_bytes += event_bytes;
    bytes_write_counter_.add(event.message.size());
  }
  if (request_events.empty()) {
    co_return;
  }
  auto request = Aws::CloudWatchLogs::Model::PutLogEventsRequest{};
  request.SetLogGroupName(args_.log_group.inner);
  request.SetLogStreamName(args_.log_stream.inner);
  request.SetLogEvents(std::move(request_events));
  auto fallback_body = std::string{};
  if (use_local_http_put_) {
    fallback_body = put_log_events_body(
      args_.log_group.inner, args_.log_stream.inner, request.GetLogEvents());
    if (http_pool_) {
      auto headers = std::map<std::string, std::string>{
        {"content-type", "application/x-amz-json-1.1"},
        {"x-amz-target", "Logs_20140328.PutLogEvents"},
      };
      auto response = co_await (*http_pool_)
                        ->post(std::string{fallback_body}, std::move(headers));
      if (response.is_ok() and response.unwrap().is_status_success()) {
        co_return;
      }
    }
  }
  auto client = *client_;
  auto outcome = co_await spawn_blocking(
    [client = std::move(client), request = std::move(request)] {
      return client->PutLogEvents(request);
    });
  if (outcome.IsSuccess()) {
    warn_rejected_events(outcome.GetResult().GetRejectedLogEventsInfo(),
                         args_.operator_location, dh);
  } else {
    if (use_local_http_put_ and http_pool_) {
      auto headers = std::map<std::string, std::string>{
        {"content-type", "application/x-amz-json-1.1"},
        {"x-amz-target", "Logs_20140328.PutLogEvents"},
      };
      auto response = co_await (*http_pool_)
                        ->post(std::string{fallback_body}, std::move(headers));
      if (response.is_ok() and response.unwrap().is_status_success()) {
        co_return;
      }
    }
    diagnostic::error("{}", aws_error("PutLogEvents", outcome.GetError()))
      .primary(args_.operator_location)
      .emit(dh);
  }
}

auto ToCloudWatch::send_hlc_batch(std::vector<Event> events,
                                  diagnostic_handler& dh) -> Task<void> {
  if (not http_pool_) {
    co_return;
  }
  auto body = std::string{"{\"events\":["};
  auto first = true;
  for (auto i = size_t{}; i < events.size(); ++i) {
    auto const& event = events[i];
    auto event_json = json_event(event);
    auto separator_size = first ? size_t{0} : size_t{1};
    if (body.size() + separator_size + event_json.size() + 2
        > max_put_request_bytes) {
      if (first) {
        diagnostic::warning(
          "CloudWatch HLC log event exceeds maximum request size")
          .primary(args_.message)
          .note("event is skipped")
          .emit(dh);
        continue;
      }
      body += "]}";
      auto headers = std::map<std::string, std::string>{
        {"authorization", fmt::format("Bearer {}", token_)},
        {"content-type", "application/json"},
      };
      auto response
        = co_await (*http_pool_)->post(std::move(body), std::move(headers));
      if (response.is_err()) {
        diagnostic::error("CloudWatch HLC request failed: {}",
                          std::move(response).unwrap_err())
          .primary(args_.operator_location)
          .emit(dh);
        co_return;
      }
      auto http_response = std::move(response).unwrap();
      if (not http_response.is_status_success()) {
        diagnostic::error("CloudWatch HLC request returned status {}",
                          http_response.status_code)
          .primary(args_.operator_location)
          .emit(dh);
        co_return;
      }
      auto rest = std::vector<Event>{};
      rest.reserve(events.size() - i);
      for (auto& pending : events | std::views::drop(i)) {
        rest.push_back(std::move(pending));
      }
      co_await send_hlc_batch(std::move(rest), dh);
      co_return;
    }
    if (not first) {
      body += ",";
    }
    first = false;
    body += event_json;
    bytes_write_counter_.add(event.message.size());
  }
  if (first) {
    co_return;
  }
  body += "]}";
  auto headers = std::map<std::string, std::string>{
    {"authorization", fmt::format("Bearer {}", token_)},
    {"content-type", "application/json"},
  };
  auto response
    = co_await (*http_pool_)->post(std::move(body), std::move(headers));
  if (response.is_err()) {
    diagnostic::error("CloudWatch HLC request failed: {}",
                      std::move(response).unwrap_err())
      .primary(args_.operator_location)
      .emit(dh);
    co_return;
  }
  auto http_response = std::move(response).unwrap();
  if (not http_response.is_status_success()) {
    diagnostic::error("CloudWatch HLC request returned status {}",
                      http_response.status_code)
      .primary(args_.operator_location)
      .emit(dh);
  }
}

auto ToCloudWatch::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  if (done_) {
    co_return FinalizeBehavior::done;
  }
  co_await flush(ctx);
  for (auto i = uint64_t{}; i < parallel_; ++i) {
    auto permit = co_await request_slots_.acquire();
    permit.forget();
  }
  co_return FinalizeBehavior::done;
}

auto ToCloudWatch::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

} // namespace tenzir::plugins::cloudwatch
