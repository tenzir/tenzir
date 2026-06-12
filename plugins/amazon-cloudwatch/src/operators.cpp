//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/operators.hpp"

#include <tenzir/as_bytes.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/eval_as.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>

#include <aws/core/utils/Outcome.h>
#include <aws/logs/model/FilterLogEventsRequest.h>
#include <aws/logs/model/FilteredLogEvent.h>
#include <aws/logs/model/GetLogEventsRequest.h>
#include <aws/logs/model/GetLogEventsResult.h>
#include <aws/logs/model/InputLogEvent.h>
#include <aws/logs/model/OutputLogEvent.h>
#include <aws/logs/model/PutLogEventsRequest.h>
#include <aws/logs/model/PutLogEventsResult.h>
#include <aws/logs/model/StartLiveTailRequest.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/Collect.h>
#include <folly/coro/WithCancellation.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <ranges>
#include <simdjson.h>
#include <tuple>

namespace tenzir::plugins::cloudwatch {

namespace {

constexpr auto live_tail_queue_capacity = uint32_t{1024};

constexpr auto max_read_events = uint64_t{10'000};
constexpr auto max_put_events = size_t{10'000};
constexpr auto max_put_request_bytes = size_t{1'048'576};
constexpr auto put_event_overhead = size_t{26};
constexpr auto max_put_event_bytes = max_put_request_bytes - put_event_overhead;
constexpr auto max_http_event_bytes = size_t{262'144};
constexpr auto max_put_batch_span = std::chrono::hours{24};
constexpr auto max_future_skew = std::chrono::hours{2};
constexpr auto max_past_skew = std::chrono::hours{24 * 14};
constexpr auto max_send_queue_capacity
  = uint64_t{std::numeric_limits<uint32_t>::max()};

auto epoch_ms(time value) -> int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
           value.time_since_epoch())
    .count();
}

auto epoch_seconds(time value) -> std::string {
  auto const ms = epoch_ms(value);
  auto sign = std::string_view{};
  auto magnitude = uint64_t{};
  if (ms < 0) {
    sign = "-";
    magnitude = static_cast<uint64_t>(-(ms + 1)) + 1;
  } else {
    magnitude = static_cast<uint64_t>(ms);
  }
  auto const seconds = magnitude / 1000;
  auto const millis = magnitude % 1000;
  if (millis == 0) {
    return fmt::format("{}{}", sign, seconds);
  }
  return fmt::format("{}{}.{:03}", sign, seconds, millis);
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

auto cloudwatch_endpoint_url(std::string_view region,
                             Option<std::string> const& endpoint)
  -> std::string {
  if (endpoint) {
    return *endpoint;
  }
  return amazon::service_endpoint_url("logs", region, "LOGS");
}

auto live_tail_endpoint(std::string_view endpoint) -> std::string {
  auto parsed = boost::urls::parse_uri_reference(endpoint);
  auto url = parsed ? boost::urls::url{*parsed} : boost::urls::url{endpoint};
  auto host = std::string{url.host()};
  if (not host.starts_with("stream-")) {
    url.set_host(fmt::format("stream-{}", host));
  }
  return std::string{url.buffer()};
}

auto valid_endpoint_url(std::string_view endpoint) -> bool {
  auto parsed = boost::urls::parse_uri_reference(endpoint);
  if (not parsed) {
    return false;
  }
  auto view = boost::urls::url_view{*parsed};
  return not view.scheme().empty() and not view.host().empty();
}

auto ingestion_path(std::string_view method) -> std::string {
  auto url = boost::urls::url{};
  url.set_path(fmt::format("/{}", method));
  return std::string{url.encoded_path()};
}

auto ingestion_headers(std::string_view content_type, std::string_view group,
                       std::string_view stream) -> std::vector<http::Header> {
  return {
    {"content-type", std::string{content_type}},
    {"x-aws-log-group", std::string{group}},
    {"x-aws-log-stream", std::string{stream}},
  };
}

auto strings_from_data(located<data> const& value) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  if (auto const* single = try_as<std::string>(&value.inner)) {
    result.push_back(*single);
    return result;
  }
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

auto selected_log_streams(FromCloudWatchArgs const& args)
  -> std::vector<std::string> {
  if (args.stream) {
    return strings_from_data(*args.stream);
  }
  return {};
}

auto selected_log_groups(FromCloudWatchArgs const& args)
  -> std::vector<std::string> {
  return strings_from_data(args.group);
}

auto selected_log_group(FromCloudWatchArgs const& args) -> std::string {
  auto groups = selected_log_groups(args);
  TENZIR_ASSERT(groups.size() == 1);
  return groups.front();
}

auto is_log_group_identifier(std::string_view group) -> bool {
  return group.starts_with("arn:");
}

auto exact_log_group_identifier(std::string group) -> std::string {
  if (is_log_group_identifier(group) and group.ends_with(":*")) {
    group.resize(group.size() - 2);
  }
  return group;
}

auto aws_error(
  std::string_view operation,
  Aws::Client::AWSError<Aws::CloudWatchLogs::CloudWatchLogsErrors> const& error)
  -> std::string {
  return fmt::format("{} failed: {} ({})", operation, error.GetMessage(),
                     error.GetExceptionName());
}

auto is_live_tail_session_timeout(
  Aws::Client::AWSError<Aws::CloudWatchLogs::CloudWatchLogsErrors> const& error)
  -> bool {
  auto const& exception = error.GetExceptionName();
  return std::string_view{exception.data(), exception.size()}
         == "SessionTimeoutException";
}

} // namespace

template <class Args>
auto make_cloudwatch_http_client(Args const& args, OpCtx& ctx, location primary,
                                 Option<std::string> endpoint,
                                 Option<std::string> token = {},
                                 bool live_tail = false)
  -> Task<std::shared_ptr<amazon::SignedHttpClient>> {
  if (endpoint and not valid_endpoint_url(*endpoint)) {
    diagnostic::error(
      "failed to initialize CloudWatch HTTP client: invalid url: {}", *endpoint)
      .primary(primary)
      .throw_();
  }
  auto aws_iam = args.aws_iam ? std::optional<located<record>>{*args.aws_iam}
                              : std::nullopt;
  auto aws_region = args.aws_region
                      ? std::optional<located<std::string>>{*args.aws_region}
                      : std::nullopt;
  auto auth = Option<ResolvedAwsIamAuth>{};
  if (not token) {
    auth = co_await resolve_aws_iam_auth(std::move(aws_iam),
                                         std::move(aws_region), ctx);
    if (not auth) {
      diagnostic::error("failed to initialize CloudWatch HTTP client")
        .primary(primary)
        .throw_();
    }
  }
  auto credentials = Option<resolved_aws_credentials>{};
  if (auth and auth->credentials) {
    credentials = std::move(*auth->credentials);
  }
  auto region_value = amazon::resolve_region(
    args.aws_region ? Option<std::string>{args.aws_region->inner} : None{},
    credentials);
  auto endpoint_value
    = endpoint ? *endpoint : cloudwatch_endpoint_url(region_value, None{});
  if (live_tail and not endpoint) {
    endpoint_value = live_tail_endpoint(endpoint_value);
  }
  auto config = amazon::SignedHttpClientConfig{
    .service = "logs",
    .region = region_value,
    .endpoint = std::move(endpoint_value),
    .sign_requests = not token,
    .credentials = std::move(credentials),
    .io_executor = ctx.io_executor(),
  };
  try {
    co_return std::make_shared<amazon::SignedHttpClient>(std::move(config));
  } catch (std::exception const& e) {
    diagnostic::error("failed to initialize CloudWatch HTTP client: {}",
                      e.what())
      .primary(primary)
      .throw_();
  }
}

namespace {

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

auto read_request_limit(FromCloudWatchArgs const& args, uint64_t emitted)
  -> Option<int> {
  if (not args.count) {
    return None{};
  }
  if (emitted >= args.count->inner) {
    return 1;
  }
  auto remaining = args.count->inner - emitted;
  return detail::narrow<int>(std::min(remaining, max_read_events));
}

auto append_filter_options(
  Aws::CloudWatchLogs::Model::FilterLogEventsRequest& request,
  FromCloudWatchArgs const& args, uint64_t emitted) -> void {
  auto group = selected_log_group(args);
  if (is_log_group_identifier(group)) {
    request.SetLogGroupIdentifier(std::move(group));
  } else {
    request.SetLogGroupName(group);
  }
  if (args.filter) {
    request.SetFilterPattern(args.filter->inner);
  }
  if (args.start) {
    request.SetStartTime(epoch_ms(args.start->inner));
  }
  if (args.end) {
    request.SetEndTime(epoch_ms(args.end->inner));
  }
  if (auto limit = read_request_limit(args, emitted)) {
    request.SetLimit(*limit);
  }
  auto log_streams = selected_log_streams(args);
  if (not log_streams.empty()) {
    request.SetLogStreamNames(make_aws_vector(log_streams));
  }
  if (args.stream_prefix) {
    request.SetLogStreamNamePrefix(args.stream_prefix->inner);
  }
  if (args.unmask) {
    request.SetUnmask(args.unmask->inner);
  }
}

auto reads_from_start(FromCloudWatchArgs const& args) -> bool {
  return args.from_start ? args.from_start->inner : false;
}

auto append_get_options(Aws::CloudWatchLogs::Model::GetLogEventsRequest& request,
                        FromCloudWatchArgs const& args, uint64_t emitted,
                        std::string_view next_token) -> void {
  auto group = selected_log_group(args);
  if (is_log_group_identifier(group)) {
    request.SetLogGroupIdentifier(exact_log_group_identifier(std::move(group)));
  } else {
    request.SetLogGroupName(group);
  }
  request.SetLogStreamName(selected_log_streams(args).front());
  if (args.start) {
    request.SetStartTime(epoch_ms(args.start->inner));
  }
  if (args.end) {
    request.SetEndTime(epoch_ms(args.end->inner));
  }
  if (auto limit = read_request_limit(args, emitted)) {
    request.SetLimit(*limit);
  }
  if (not next_token.empty()) {
    request.SetStartFromHead(reads_from_start(args));
  } else if (args.from_start) {
    request.SetStartFromHead(args.from_start->inner);
  }
  if (args.unmask) {
    request.SetUnmask(args.unmask->inner);
  }
}

auto live_tail_request(FromCloudWatchArgs const& args)
  -> Aws::CloudWatchLogs::Model::StartLiveTailRequest {
  auto request = Aws::CloudWatchLogs::Model::StartLiveTailRequest{};
  auto groups = selected_log_groups(args);
  request.SetLogGroupIdentifiers(make_aws_vector(groups));
  if (args.filter) {
    request.SetLogEventFilterPattern(args.filter->inner);
  }
  auto log_streams = selected_log_streams(args);
  if (not log_streams.empty()) {
    request.SetLogStreamNames(make_aws_vector(log_streams));
  }
  if (args.stream_prefix) {
    request.SetLogStreamNamePrefixes(
      Aws::Vector<Aws::String>{Aws::String{args.stream_prefix->inner}});
  }
  return request;
}

auto live_tail_page(
  Aws::CloudWatchLogs::Model::LiveTailSessionUpdate const& update)
  -> SourcePage {
  auto page = SourcePage{};
  for (auto const& event : update.GetSessionResults()) {
    page.events.push_back(CloudWatchEvent{
      .timestamp = time{std::chrono::milliseconds{event.GetTimestamp()}},
      .ingestion_time
      = time{std::chrono::milliseconds{event.GetIngestionTime()}},
      .log_group = amazon::from_aws_string(event.GetLogGroupIdentifier()),
      .log_stream = amazon::from_aws_string(event.GetLogStreamName()),
      .message = amazon::from_aws_string(event.GetMessage()),
      .event_id = std::nullopt,
    });
  }
  return page;
}

auto hlc_json_event(ToCloudWatch::Event const& event) -> std::string {
  auto result = std::string{"{\"time\":"};
  result += epoch_seconds(event.timestamp);
  result += R"(,"event":)";
  result += detail::json_escape(event.message);
  result += "}";
  return result;
}

auto max_event_bytes(ToMethod method) -> size_t {
  if (method == ToMethod::put) {
    return max_put_event_bytes;
  }
  return max_http_event_bytes;
}

auto structured_json_event(ToCloudWatch::Event const& event) -> std::string {
  auto payload = simdjson::padded_string{event.message};
  auto parser = simdjson::dom::parser{};
  auto parsed = parser.parse(payload);
  auto result = std::string{"{\"timestamp\":"};
  result += std::to_string(epoch_ms(event.timestamp));
  result += R"(,"message":)";
  if (parsed.error()) {
    result += detail::json_escape(event.message);
  } else {
    result += event.message;
  }
  result += "}";
  return result;
}

auto cloudwatch_warning(ToCloudWatchDiagnosticPrimary primary,
                        std::string message,
                        std::vector<std::string> notes = {})
  -> ToCloudWatchDiagnostic {
  return {
    .severity = ToCloudWatchDiagnosticSeverity::warning,
    .primary = primary,
    .message = std::move(message),
    .notes = std::move(notes),
  };
}

auto cloudwatch_error(std::string message) -> ToCloudWatchDiagnostic {
  return {
    .severity = ToCloudWatchDiagnosticSeverity::error,
    .primary = ToCloudWatchDiagnosticPrimary::operator_,
    .message = std::move(message),
  };
}

auto merge_report(ToCloudWatchSendReport& result, ToCloudWatchSendReport next)
  -> void {
  result.bytes += next.bytes;
  result.events += next.events;
  result.failed = result.failed or next.failed;
  result.diagnostics.insert(result.diagnostics.end(),
                            std::make_move_iterator(next.diagnostics.begin()),
                            std::make_move_iterator(next.diagnostics.end()));
}

auto rejected_event_warnings(
  Aws::CloudWatchLogs::Model::RejectedLogEventsInfo const& rejected)
  -> std::vector<ToCloudWatchDiagnostic> {
  auto result = std::vector<ToCloudWatchDiagnostic>{};
  if (rejected.TooNewLogEventStartIndexHasBeenSet()) {
    result.push_back(cloudwatch_warning(
      ToCloudWatchDiagnosticPrimary::operator_,
      "CloudWatch rejected too-new log events",
      {fmt::format("events start at batch index {}",
                   rejected.GetTooNewLogEventStartIndex())}));
  }
  if (rejected.TooOldLogEventEndIndexHasBeenSet()) {
    result.push_back(
      cloudwatch_warning(ToCloudWatchDiagnosticPrimary::operator_,
                         "CloudWatch rejected too-old log events",
                         {fmt::format("events end before batch index {}",
                                      rejected.GetTooOldLogEventEndIndex())}));
  }
  if (rejected.ExpiredLogEventEndIndexHasBeenSet()) {
    result.push_back(
      cloudwatch_warning(ToCloudWatchDiagnosticPrimary::operator_,
                         "CloudWatch rejected expired log events",
                         {fmt::format("events end before batch index {}",
                                      rejected.GetExpiredLogEventEndIndex())}));
  }
  return result;
}

struct AcceptedPutMetrics {
  size_t bytes = 0;
  size_t events = 0;
};

auto accepted_put_metrics(
  Aws::CloudWatchLogs::Model::RejectedLogEventsInfo const& rejected,
  std::vector<size_t> const& event_bytes) -> AcceptedPutMetrics {
  auto rejected_events = std::vector<bool>(event_bytes.size(), false);
  auto reject_prefix = [&](auto index) {
    if (index < 0) {
      return;
    }
    auto end = std::min(static_cast<size_t>(index) + 1, event_bytes.size());
    std::fill(rejected_events.begin(), rejected_events.begin() + end, true);
  };
  if (rejected.ExpiredLogEventEndIndexHasBeenSet()) {
    reject_prefix(rejected.GetExpiredLogEventEndIndex());
  }
  if (rejected.TooOldLogEventEndIndexHasBeenSet()) {
    reject_prefix(rejected.GetTooOldLogEventEndIndex());
  }
  if (rejected.TooNewLogEventStartIndexHasBeenSet()) {
    auto index = rejected.GetTooNewLogEventStartIndex();
    if (index >= 0) {
      auto begin = std::min(static_cast<size_t>(index), event_bytes.size());
      std::fill(rejected_events.begin() + begin, rejected_events.end(), true);
    }
  }
  auto result = AcceptedPutMetrics{};
  for (auto i = size_t{}; i < event_bytes.size(); ++i) {
    if (rejected_events[i]) {
      continue;
    }
    result.bytes += event_bytes[i];
    ++result.events;
  }
  return result;
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

struct AcceptedHttpMetrics {
  size_t bytes = 0;
  size_t events = 0;
};

auto accepted_http_metrics(ToMethod method, std::string const& body,
                           std::vector<size_t> const& event_bytes,
                           ToCloudWatchSendReport& report)
  -> AcceptedHttpMetrics {
  auto request_events = event_bytes.size();
  auto result = AcceptedHttpMetrics{.events = request_events};
  for (auto bytes : event_bytes) {
    result.bytes += bytes;
  }
  if ((method != ToMethod::json and method != ToMethod::ndjson)
      or body.empty()) {
    return result;
  }
  auto parser = simdjson::dom::parser{};
  auto doc = parser.parse(body);
  if (doc.error()) {
    return result;
  }
  auto partial = doc["partialSuccess"];
  if (partial.error()) {
    return result;
  }
  auto partial_obj = simdjson::dom::element{};
  if (partial.get(partial_obj) != simdjson::SUCCESS) {
    return result;
  }
  auto rejected = int_from_json(partial_obj, "rejectedLogRecords");
  if (rejected <= 0) {
    return result;
  }
  auto rejected_events
    = std::min(static_cast<size_t>(rejected), request_events);
  result = AcceptedHttpMetrics{.events = request_events - rejected_events};
  auto notes = std::vector<std::string>{fmt::format(
    "{} of {} events were rejected", rejected_events, request_events)};
  notes.push_back("accepted byte metrics are omitted because CloudWatch does "
                  "not report rejected event indexes");
  auto error_message = string_from_json(partial_obj, "errorMessage");
  if (not error_message.empty()) {
    notes.push_back(std::move(error_message));
  }
  report.diagnostics.push_back(cloudwatch_warning(
    ToCloudWatchDiagnosticPrimary::operator_,
    "CloudWatch rejected HTTP log events", std::move(notes)));
  return result;
}

auto parse_filter_log_events_response(std::string const& body,
                                      std::string const& log_group,
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
  page.next_token = string_from_json(doc.value(), "nextToken");
  page.done = page.next_token.empty() or page.next_token == previous_token;
  auto events = doc["events"];
  if (events.error()) {
    return page;
  }
  for (auto event : events) {
    auto message = string_from_json(event, "message");
    page.events.push_back(CloudWatchEvent{
      .timestamp
      = time{std::chrono::milliseconds{int_from_json(event, "timestamp")}},
      .ingestion_time
      = time{std::chrono::milliseconds{int_from_json(event, "ingestionTime")}},
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
                                   std::string const& previous_token,
                                   bool from_start) -> SourcePage {
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
  page.next_token = string_from_json(
    doc.value(), from_start ? "nextForwardToken" : "nextBackwardToken");
  page.done = page.next_token.empty() or page.next_token == previous_token;
  auto events = doc["events"];
  if (events.error()) {
    return page;
  }
  for (auto event : events) {
    page.events.push_back(CloudWatchEvent{
      .timestamp
      = time{std::chrono::milliseconds{int_from_json(event, "timestamp")}},
      .ingestion_time
      = time{std::chrono::milliseconds{int_from_json(event, "ingestionTime")}},
      .log_group = log_group,
      .log_stream = log_stream,
      .message = string_from_json(event, "message"),
      .event_id = std::nullopt,
    });
  }
  return page;
}

auto send_put_batch(amazon::SignedHttpClient& client,
                    ToCloudWatchArgs const& args,
                    std::vector<ToCloudWatch::Event> events)
  -> Task<ToCloudWatchSendReport> {
  auto report = ToCloudWatchSendReport{};
  std::ranges::sort(events, {}, &ToCloudWatch::Event::timestamp);
  auto now = std::chrono::system_clock::now();
  auto request_events
    = Aws::Vector<Aws::CloudWatchLogs::Model::InputLogEvent>{};
  auto request_bytes = size_t{};
  auto request_message_bytes = std::vector<size_t>{};
  auto first_timestamp = Option<time>{};
  for (auto i = size_t{}; i < events.size(); ++i) {
    auto& event = events[i];
    if (event.timestamp > now + max_future_skew
        or event.timestamp < now - max_past_skew) {
      report.diagnostics.push_back(cloudwatch_warning(
        ToCloudWatchDiagnosticPrimary::operator_,
        "CloudWatch log event timestamp is outside the accepted time window",
        {"event is skipped"}));
      continue;
    }
    auto event_bytes = event.message.size() + put_event_overhead;
    auto exceeds_batch_span
      = first_timestamp
        and event.timestamp - *first_timestamp > max_put_batch_span;
    if (request_events.size() >= max_put_events
        or request_bytes + event_bytes > max_put_request_bytes
        or exceeds_batch_span) {
      auto rest = std::vector<ToCloudWatch::Event>{};
      rest.reserve(events.size() - i);
      for (auto& pending : events | std::views::drop(i)) {
        rest.push_back(std::move(pending));
      }
      merge_report(report,
                   co_await send_put_batch(client, args, std::move(rest)));
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
    request_message_bytes.push_back(event.message.size());
  }
  if (request_events.empty()) {
    co_return report;
  }
  auto request = Aws::CloudWatchLogs::Model::PutLogEventsRequest{};
  request.SetLogGroupName(args.log_group.inner);
  request.SetLogStreamName(args.log_stream->inner);
  request.SetLogEvents(std::move(request_events));
  auto response = co_await client.api_call("PutLogEvents", request);
  if (response.is_err()) {
    report.failed = true;
    report.diagnostics.push_back(
      cloudwatch_error(std::move(response).unwrap_err()));
    co_return report;
  }
  auto http_response = std::move(response).unwrap();
  auto aws_result = amazon::to_aws_json_result(std::move(http_response));
  auto result
    = Aws::CloudWatchLogs::Model::PutLogEventsResult{std::move(aws_result)};
  auto warnings = rejected_event_warnings(result.GetRejectedLogEventsInfo());
  report.diagnostics.insert(report.diagnostics.end(),
                            std::make_move_iterator(warnings.begin()),
                            std::make_move_iterator(warnings.end()));
  auto accepted = accepted_put_metrics(result.GetRejectedLogEventsInfo(),
                                       request_message_bytes);
  report.bytes += accepted.bytes;
  report.events += accepted.events;
  co_return report;
}

auto post_ingest(amazon::SignedHttpClient& client, Option<std::string> token,
                 std::string path, std::string body,
                 std::vector<http::Header> headers, std::string_view operation)
  -> Task<Result<http::Response, std::string>> {
  if (token) {
    http::set(headers, "authorization", fmt::format("Bearer {}", *token));
    co_return co_await client.post_unsigned(std::move(path), std::move(body),
                                            std::move(headers), operation);
  }
  co_return co_await client.post(std::move(path), std::move(body),
                                 std::move(headers), operation);
}

auto send_http_batch(amazon::SignedHttpClient& client,
                     ToCloudWatchArgs const& args, ToMethod method,
                     Option<std::string> token,
                     std::vector<ToCloudWatch::Event> events)
  -> Task<ToCloudWatchSendReport> {
  auto report = ToCloudWatchSendReport{};
  auto const stream = args.log_stream->inner;
  auto path = std::string{};
  auto content_type = std::string{};
  auto format_event = [](ToMethod method, ToCloudWatch::Event const& event) {
    if (method == ToMethod::hlc) {
      return hlc_json_event(event);
    }
    return structured_json_event(event);
  };
  switch (method) {
    case ToMethod::hlc:
      path = ingestion_path("services/collector/event");
      content_type = "application/json";
      break;
    case ToMethod::ndjson:
      path = ingestion_path("ingest/bulk");
      content_type = "application/x-ndjson";
      break;
    case ToMethod::json:
      path = ingestion_path("ingest/json");
      content_type = "application/json";
      break;
    case ToMethod::put:
      TENZIR_UNREACHABLE();
  }
  auto send_request
    = [&](std::string request_body,
          std::vector<size_t> request_event_bytes) -> Task<bool> {
    auto headers
      = ingestion_headers(content_type, args.log_group.inner, stream);
    auto response
      = co_await post_ingest(client, token, path, std::move(request_body),
                             std::move(headers), "CloudWatch ingestion");
    if (response.is_err()) {
      report.failed = true;
      report.diagnostics.push_back(
        cloudwatch_error(std::move(response).unwrap_err()));
      co_return false;
    }
    auto http_response = std::move(response).unwrap();
    auto accepted = accepted_http_metrics(method, http_response.body,
                                          request_event_bytes, report);
    report.bytes += accepted.bytes;
    report.events += accepted.events;
    co_return true;
  };
  auto body = std::string{};
  if (method == ToMethod::hlc or method == ToMethod::json) {
    body = "[";
  }
  auto first = true;
  auto request_event_bytes = std::vector<size_t>{};
  for (auto i = size_t{}; i < events.size(); ++i) {
    auto const& event = events[i];
    auto event_json = format_event(method, event);
    auto separator_size = first ? size_t{0} : size_t{1};
    auto trailer_size = size_t{};
    if (method == ToMethod::hlc or method == ToMethod::json) {
      trailer_size = 1;
    }
    if (method != ToMethod::put and event_json.size() > max_http_event_bytes) {
      report.diagnostics.push_back(cloudwatch_warning(
        ToCloudWatchDiagnosticPrimary::payload,
        "CloudWatch HTTP log event exceeds maximum event size",
        {"event is skipped"}));
      continue;
    }
    if (request_event_bytes.size() >= max_put_events
        or body.size() + separator_size + event_json.size() + trailer_size
             > max_put_request_bytes) {
      if (first) {
        report.diagnostics.push_back(cloudwatch_warning(
          ToCloudWatchDiagnosticPrimary::payload,
          "CloudWatch HTTP log event exceeds maximum request size",
          {"event is skipped"}));
        continue;
      }
      if (method == ToMethod::hlc or method == ToMethod::json) {
        body += "]";
      }
      if (not(co_await send_request(std::move(body),
                                    std::move(request_event_bytes)))) {
        co_return report;
      }
      auto rest = std::vector<ToCloudWatch::Event>{};
      rest.reserve(events.size() - i);
      for (auto& pending : events | std::views::drop(i)) {
        rest.push_back(std::move(pending));
      }
      merge_report(report, co_await send_http_batch(client, args, method, token,
                                                    std::move(rest)));
      co_return report;
    }
    if (not first) {
      body += method == ToMethod::ndjson ? "\n" : ",";
    }
    first = false;
    body += event_json;
    request_event_bytes.push_back(event.message.size());
  }
  if (first) {
    co_return report;
  }
  if (method == ToMethod::hlc or method == ToMethod::json) {
    body += "]";
  }
  std::ignore
    = co_await send_request(std::move(body), std::move(request_event_bytes));
  co_return report;
}

} // namespace

auto default_to_amazon_cloudwatch_message_expression() -> ast::expression {
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

auto FromCloudWatch::snapshot(Serde& serde) -> void {
  serde("next_token", next_token_);
  serde("emitted", emitted_);
  serde("done", done_);
}

auto FromCloudWatch::start(OpCtx& ctx) -> Task<void> {
  if (args_.mode.inner == "search") {
    mode_ = FromMode::search;
  } else if (args_.mode.inner == "replay") {
    mode_ = FromMode::replay;
  } else {
    mode_ = FromMode::live;
  }
  auto endpoint = amazon::endpoint_override("LOGS");
  try {
    client_
      = co_await make_cloudwatch_http_client(args_, ctx, args_.group.source,
                                             endpoint, None{},
                                             mode_ == FromMode::live);
  } catch (diagnostic& d) {
    ctx.dh().emit(std::move(d));
    done_ = true;
    co_return;
  }
  bytes_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_cloudwatch"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::bytes);
  events_read_counter_
    = ctx.make_counter(MetricsLabel{"operator", "from_amazon_cloudwatch"},
                       MetricsDirection::read, MetricsVisibility::external_,
                       MetricsUnit::events);
  if (mode_ == FromMode::live) {
    auto queue = Arc<folly::coro::BoundedQueue<SourcePage, false, true>>{
      std::in_place,
      live_tail_queue_capacity,
    };
    live_queue_ = queue;
    auto args = args_;
    auto client = client_;
    auto cancel = live_cancel_.getToken();
    ctx.spawn_task([client = std::move(client), args = std::move(args),
                    queue = std::move(queue), cancel]() mutable -> Task<void> {
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token, cancel);
      co_await folly::coro::co_withCancellation(
        token,
        folly::coro::co_invoke(
          [client = std::move(client), args = std::move(args),
           queue = std::move(queue)]() mutable -> Task<void> {
            while (true) {
              auto request = live_tail_request(args);
              auto pending_pages = std::vector<SourcePage>{};
              auto pending_error = std::string{};
              auto session_expired = false;
              auto stream_error = false;
              auto handler = Aws::CloudWatchLogs::Model::StartLiveTailHandler{};
              handler.SetLiveTailSessionUpdateCallback(
                [&](Aws::CloudWatchLogs::Model::LiveTailSessionUpdate const&
                      update) {
                  pending_pages.push_back(live_tail_page(update));
                });
              handler.SetOnErrorCallback(
                [&](Aws::Client::AWSError<
                    Aws::CloudWatchLogs::CloudWatchLogsErrors> const& error) {
                  if (is_live_tail_session_timeout(error)) {
                    session_expired = true;
                    return;
                  }
                  pending_error = aws_error("StartLiveTail", error);
                });
              request.SetEventStreamHandler(handler);
              auto callbacks = HttpStreamCallbacks{};
              callbacks.on_headers = [&](http::Response const& response) {
                if (not response.is_status_success()) {
                  pending_error = fmt::format("StartLiveTail returned HTTP {}",
                                              response.status_code);
                }
              };
              callbacks.on_body = [&](std::string chunk) mutable -> Task<bool> {
                if (session_expired) {
                  co_return true;
                }
                if (not pending_error.empty()) {
                  stream_error = true;
                  co_return true;
                }
                auto bytes = Aws::Utils::ByteBuffer{
                  reinterpret_cast<unsigned char const*>(chunk.data()),
                  chunk.size(),
                };
                request.GetEventStreamDecoder().Pump(bytes);
                for (auto& page : pending_pages) {
                  co_await queue->enqueue(std::move(page));
                }
                pending_pages.clear();
                if (session_expired) {
                  co_return true;
                }
                if (not pending_error.empty()) {
                  stream_error = true;
                  co_await queue->enqueue(SourcePage{
                    .error = std::exchange(pending_error, {}),
                    .done = true,
                  });
                  co_return true;
                }
                co_return false;
              };
              auto payload = request.SerializePayload();
              auto terminal_error = std::string{};
              try {
                auto result = co_await client->stream_post(
                  "/", std::string{payload.c_str(), payload.size()},
                  request.GetHeaders(), std::move(callbacks), "StartLiveTail");
                if (result.is_err()) {
                  if (not session_expired) {
                    terminal_error = std::move(result).unwrap_err();
                  }
                } else {
                  auto response = std::move(result).unwrap();
                  if (not response.is_status_success()) {
                    terminal_error = fmt::format(
                      "StartLiveTail returned HTTP {}", response.status_code);
                  }
                }
              } catch (std::exception const& e) {
                if (not session_expired) {
                  terminal_error
                    = fmt::format("StartLiveTail request failed: {}", e.what());
                }
              }
              if (not terminal_error.empty()) {
                co_await queue->enqueue(SourcePage{
                  .error = std::move(terminal_error),
                  .done = true,
                });
                co_return;
              }
              if (stream_error) {
                co_return;
              }
            }
          }));
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
    co_return Any{co_await (*live_queue_)->dequeue()};
  }
  auto args = args_;
  auto token = next_token_;
  auto mode = mode_;
  auto emitted = emitted_;
  if (mode == FromMode::search) {
    auto request = Aws::CloudWatchLogs::Model::FilterLogEventsRequest{};
    append_filter_options(request, args, emitted);
    if (not token.empty()) {
      request.SetNextToken(token);
    }
    auto response = co_await client_->api_call("FilterLogEvents", request);
    if (response.is_err()) {
      co_return SourcePage{.error = std::move(response).unwrap_err(),
                           .done = true};
    }
    auto http_response = std::move(response).unwrap();
    co_return parse_filter_log_events_response(
      http_response.body, selected_log_group(args_), token);
  }
  auto request = Aws::CloudWatchLogs::Model::GetLogEventsRequest{};
  append_get_options(request, args, emitted, token);
  if (not token.empty()) {
    request.SetNextToken(token);
  }
  auto response = co_await client_->api_call("GetLogEvents", request);
  if (response.is_err()) {
    co_return SourcePage{.error = std::move(response).unwrap_err(),
                         .done = true};
  }
  auto http_response = std::move(response).unwrap();
  co_return parse_get_log_events_response(http_response.body,
                                          selected_log_group(args_),
                                          selected_log_streams(args_).front(),
                                          token, reads_from_start(args_));
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
  if (args_.count) {
    auto remaining = args_.count->inner - emitted_;
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
    auto const rows = slice.rows();
    emitted_ += rows;
    co_await push(std::move(slice));
    events_read_counter_.add(rows);
  }
  if (args_.count and emitted_ >= args_.count->inner) {
    done_ = true;
  }
}

auto FromCloudWatch::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

auto FromCloudWatch::stop(OpCtx& ctx) -> Task<void> {
  TENZIR_UNUSED(ctx);
  live_cancel_.requestCancellation();
  if (live_queue_) {
    std::ignore = (*live_queue_)->try_enqueue(SourcePage{.done = true});
  }
  done_ = true;
  co_return;
}

ToCloudWatch::ToCloudWatch(ToCloudWatchArgs args)
  : args_{std::move(args)}, request_slots_{1} {
}

auto ToCloudWatch::start(OpCtx& ctx) -> Task<void> {
  if (args_.method.inner == "hlc") {
    method_ = ToMethod::hlc;
  } else if (args_.method.inner == "ndjson") {
    method_ = ToMethod::ndjson;
  } else if (args_.method.inner == "json") {
    method_ = ToMethod::json;
  } else {
    method_ = ToMethod::put;
  }
  batch_size_ = args_.batch_size ? args_.batch_size->inner : uint64_t{1000};
  batch_timeout_ = args_.batch_timeout ? args_.batch_timeout->inner
                                       : std::chrono::seconds{1};
  parallel_ = args_.parallel ? args_.parallel->inner : uint64_t{1};
  if (parallel_ == 0 or parallel_ >= max_send_queue_capacity) {
    auto primary
      = args_.parallel ? args_.parallel->source : args_.operator_location;
    if (parallel_ == 0) {
      diagnostic::error("parallel must be greater than zero")
        .primary(primary)
        .emit(ctx);
    } else {
      diagnostic::error("parallel must be less than {}",
                        max_send_queue_capacity)
        .primary(primary)
        .emit(ctx);
    }
    done_ = true;
    co_return;
  }
  request_slots_ = Semaphore{detail::narrow<size_t>(parallel_)};
  send_queue_ = Arc<folly::coro::BoundedQueue<ToCloudWatchSendReport>>{
    std::in_place,
    detail::narrow<uint32_t>(parallel_ + 1),
  };
  auto operator_label = MetricsLabel{"operator", "to_amazon_cloudwatch"};
  bytes_write_counter_
    = ctx.make_counter(operator_label, MetricsDirection::write,
                       MetricsVisibility::external_, MetricsUnit::bytes);
  events_write_counter_
    = ctx.make_counter(operator_label, MetricsDirection::write,
                       MetricsVisibility::external_, MetricsUnit::events);
  auto token = Option<std::string>{};
  if (args_.token) {
    auto requests = std::vector<secret_request>{
      make_secret_request("token", *args_.token, token_, ctx.dh()),
    };
    if (auto result = co_await ctx.resolve_secrets(std::move(requests));
        result.is_error()) {
      done_ = true;
      co_return;
    }
    token = token_;
  }
  auto endpoint = Option<std::string>{};
  if (args_.endpoint) {
    endpoint = args_.endpoint->inner;
  } else {
    endpoint = amazon::endpoint_override("LOGS");
  }
  try {
    auto primary
      = args_.endpoint ? args_.endpoint->source : args_.log_group.source;
    client_ = co_await make_cloudwatch_http_client(args_, ctx, primary,
                                                   endpoint, std::move(token));
  } catch (diagnostic& d) {
    ctx.dh().emit(std::move(d));
    done_ = true;
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
        return time{std::chrono::duration_cast<time::duration>(
          std::chrono::system_clock::now().time_since_epoch())};
      });
  auto consume_timestamps = [&](int64_t count) {
    for (auto i = int64_t{}; i < count; ++i) {
      std::ignore = timestamps.next();
    }
  };
  for (auto const& messages : eval(args_.payload, input, dh)) {
    auto append = [&](auto const& array) -> Task<void> {
      for (auto i = int64_t{0}; i < array.length(); ++i) {
        auto t = time{std::chrono::duration_cast<time::duration>(
          std::chrono::system_clock::now().time_since_epoch())};
        if (auto next = timestamps.next(); next and *next) {
          t = **next;
        }
        if (array.IsNull(i)) {
          diagnostic::warning("expected `string` or `blob`, got `null`")
            .primary(args_.payload)
            .note("event is skipped")
            .emit(dh);
          continue;
        }
        auto bytes = as_bytes(array.Value(i));
        auto message = std::string{reinterpret_cast<char const*>(bytes.data()),
                                   bytes.size()};
        if (message.size() > max_event_bytes(method_)) {
          diagnostic::warning(
            "CloudWatch log event exceeds maximum payload size")
            .primary(args_.payload)
            .note("event is skipped")
            .emit(dh);
          continue;
        }
        if (batch_.empty()) {
          next_timeout_ = std::chrono::steady_clock::now() + batch_timeout_;
          if (not timer_armed_) {
            arm_flush_timer(ctx);
          }
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
    consume_timestamps(messages.length());
    diagnostic::warning("expected `string` or `blob`, got `{}`",
                        messages.type.kind())
      .primary(args_.payload)
      .note("events are skipped")
      .emit(dh);
  }
}

auto ToCloudWatch::flush(OpCtx& ctx) -> Task<void> {
  if (batch_.empty()) {
    co_return;
  }
  if (not client_) {
    co_return;
  }
  TENZIR_ASSERT(send_queue_);
  auto events = std::exchange(batch_, {});
  next_timeout_ = None{};
  drain_send_reports(ctx);
  auto permit = Option<SemaphorePermit>{};
  while (not permit) {
    permit = request_slots_.try_acquire();
    if (permit) {
      break;
    }
    auto report = co_await (*send_queue_)->dequeue();
    handle_send_report(std::move(report), ctx);
  }
  if (done_) {
    batch_ = std::move(events);
    co_return;
  }
  auto args = args_;
  auto client = client_;
  auto method = method_;
  auto queue = *send_queue_;
  auto wakeup = wakeup_queue_;
  auto token = args_.token ? Option<std::string>{token_} : None{};
  ++pending_reports_;
  ctx.spawn_task([client = std::move(client), args = std::move(args), method,
                  token = std::move(token), events = std::move(events),
                  queue = std::move(queue), wakeup = std::move(wakeup),
                  permit = std::move(permit)]() mutable -> Task<void> {
    auto report = ToCloudWatchSendReport{};
    try {
      if (method == ToMethod::put) {
        report = co_await send_put_batch(*client, args, std::move(events));
      } else {
        report = co_await send_http_batch(*client, args, method, token,
                                          std::move(events));
      }
    } catch (std::exception const& e) {
      report.failed = true;
      report.diagnostics.push_back(cloudwatch_error(
        fmt::format("CloudWatch request failed: {}", e.what())));
    }
    permit->release();
    co_await queue->enqueue(std::move(report));
    co_await wakeup->enqueue(Any{ToCloudWatchReportReady{}});
  });
}

auto ToCloudWatch::await_task(diagnostic_handler& dh) const -> Task<Any> {
  TENZIR_UNUSED(dh);
  co_return co_await wakeup_queue_->dequeue();
}

auto ToCloudWatch::arm_flush_timer(OpCtx& ctx) -> void {
  TENZIR_ASSERT(next_timeout_);
  timer_armed_ = true;
  ctx.spawn_task(
    [queue = wakeup_queue_, deadline = *next_timeout_]() mutable -> Task<void> {
      co_await sleep_until(deadline);
      co_await queue->enqueue(Any{ToCloudWatchFlushTimeout{}});
    });
}

auto ToCloudWatch::handle_send_report(ToCloudWatchSendReport report, OpCtx& ctx)
  -> void {
  for (auto& item : report.diagnostics) {
    auto emit = [&](auto diag) {
      if (item.primary == ToCloudWatchDiagnosticPrimary::payload) {
        diag = std::move(diag).primary(args_.payload);
      } else {
        diag = std::move(diag).primary(args_.operator_location);
      }
      for (auto& note : item.notes) {
        diag = std::move(diag).note("{}", note);
      }
      std::move(diag).emit(ctx);
    };
    if (item.severity == ToCloudWatchDiagnosticSeverity::error) {
      emit(diagnostic::error("{}", item.message));
    } else {
      emit(diagnostic::warning("{}", item.message));
    }
  }
  bytes_write_counter_.add(report.bytes);
  events_write_counter_.add(report.events);
  TENZIR_ASSERT(pending_reports_ > 0);
  --pending_reports_;
  done_ = done_ or report.failed;
}

auto ToCloudWatch::drain_send_reports(OpCtx& ctx) -> void {
  if (not send_queue_) {
    return;
  }
  while (auto report = (*send_queue_)->try_dequeue()) {
    handle_send_report(std::move(*report), ctx);
  }
}

auto ToCloudWatch::process_task(Any result, OpCtx& ctx) -> Task<void> {
  if (result.try_as<ToCloudWatchReportReady>()) {
    drain_send_reports(ctx);
    co_return;
  }
  TENZIR_ASSERT(result.try_as<ToCloudWatchFlushTimeout>());
  timer_armed_ = false;
  if (next_timeout_ and std::chrono::steady_clock::now() >= *next_timeout_) {
    co_await flush(ctx);
  }
  if (not done_ and next_timeout_) {
    // The timer was armed for a batch that has since been flushed by size;
    // re-arm it for the batch that started afterwards.
    arm_flush_timer(ctx);
  }
}

auto ToCloudWatch::wait_for_requests(OpCtx& ctx) -> Task<void> {
  TENZIR_ASSERT(send_queue_);
  while (pending_reports_ > 0) {
    auto report = co_await (*send_queue_)->dequeue();
    handle_send_report(std::move(report), ctx);
  }
  drain_send_reports(ctx);
  co_return;
}

auto ToCloudWatch::prepare_snapshot(OpCtx& ctx) -> Task<void> {
  if (not send_queue_) {
    co_return;
  }
  if (not done_) {
    co_await flush(ctx);
  }
  co_await wait_for_requests(ctx);
}

auto ToCloudWatch::finalize(OpCtx& ctx) -> Task<FinalizeBehavior> {
  if (not send_queue_) {
    co_return FinalizeBehavior::done;
  }
  if (not done_) {
    co_await flush(ctx);
  }
  co_await wait_for_requests(ctx);
  done_ = true;
  co_return FinalizeBehavior::done;
}

auto ToCloudWatch::state() -> OperatorState {
  return done_ ? OperatorState::done : OperatorState::normal;
}

} // namespace tenzir::plugins::cloudwatch
