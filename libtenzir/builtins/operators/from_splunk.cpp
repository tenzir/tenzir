//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arc.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/location.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/json_parser.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/operator/optimization.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/pushdown/read.hpp"
#include "tenzir/pushdown/splunk.hpp"
#include "tenzir/startup_argument.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"
#include "tenzir/variant.hpp"

#include <boost/url/parse.hpp>
#include <caf/error.hpp>
#include <caf/message.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/WithCancellation.h>

#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_splunk {

namespace {

constexpr auto export_path
  = std::string_view{"/services/search/v2/jobs/export"};

/// The key of the subpipeline that aggregates locally.
constexpr auto aggregation_sub_key = int64_t{0};

struct FromSplunkArgs {
  located<std::string> url;
  // Exactly one of `search` and `index` is set.
  Option<located<std::string>> search;
  Option<located<data>> index;
  located<data> earliest;
  located<data> latest;
  Option<located<data>> options;
  StartupArgument<nova::Record> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  Option<location> raw;
  location operator_location = location::unknown;
  // Filters, a limit, a projection, and an aggregation from downstream
  // operators. Prefilters, a projection, and a limit go into the search as
  // trailing stages (see `pushdown::plan_spl_search`), and the operator applies
  // all of them locally. An aggregation goes into the search as `stats` stages
  // (see `pushdown::plan_spl_aggregation`), or runs locally.
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection, opt::Aggregation>
    optimization;
};

struct SplunkMessage {
  std::string type;
  std::string text;
};

auto is_realtime_time_bound(std::string_view value) -> bool {
  value = detail::trim(value);
  if (value.size() < 2
      or not detail::ascii_icase_equal(value.substr(0, 2), "rt")) {
    return false;
  }
  return value.size() == 2 or value[2] == '+' or value[2] == '-'
         or value[2] == '@';
}

auto validate_time_bound(std::string_view name, data const& value,
                         location source, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auto const* string = try_as<std::string>(value)) {
    TRY(check_non_empty(name, located<std::string>{*string, source}, dh));
    if (is_realtime_time_bound(*string)) {
      diagnostic::error("real-time Splunk searches are not supported")
        .primary(source, "`{}` uses a real-time bound", name)
        .note("use a finite time bound instead")
        .emit(dh);
      return failure::promise();
    }
    return {};
  }
  if (is<time>(value)) {
    return {};
  }
  auto const inferred = type::infer(value);
  diagnostic::error("expected `string` or `time` for `{}`", name)
    .primary(source, "got `{}`", inferred ? inferred->kind() : type_kind{})
    .emit(dh);
  return failure::promise();
}

auto is_protected_option(std::string_view name) -> bool {
  return detail::ascii_icase_equal(name, "search")
         or detail::ascii_icase_equal(name, "earliest_time")
         or detail::ascii_icase_equal(name, "latest_time")
         or detail::ascii_icase_equal(name, "output_mode")
         or detail::ascii_icase_equal(name, "preview");
}

auto is_realtime_only_option(std::string_view name) -> bool {
  auto const normalized = detail::ascii_tolower(name);
  return normalized.starts_with("rt_") or normalized == "indexedrealtime"
         or normalized == "indexedrealtimeoffset"
         or normalized == "realtime_schedule" or normalized == "replay_et"
         or normalized == "replay_lt" or normalized == "replay_speed";
}

auto validate_options(located<data> const& value, diagnostic_handler& dh)
  -> failure_or<void> {
  auto const source = value.source;
  auto const* options = try_as<record>(value.inner);
  if (not options) {
    auto const inferred = type::infer(value.inner);
    diagnostic::error("expected `record` for `options`")
      .primary(source, "got `{}`", inferred ? inferred->kind() : type_kind{})
      .emit(dh);
    return failure::promise();
  }
  for (auto const& [name, option] : *options) {
    if (is<list>(option)) {
      diagnostic::error("list-valued Splunk options are not supported")
        .primary(source, "`options.{}` contains a list", name)
        .note("pass a single value for `{}`", name)
        .emit(dh);
      return failure::promise();
    }
    if (is_protected_option(name)) {
      diagnostic::error("`options` must not override `{}`", name)
        .primary(source)
        .emit(dh);
      return failure::promise();
    }
    if (is_realtime_only_option(name)) {
      diagnostic::error("real-time Splunk searches are not supported")
        .primary(source, "`options` contains the real-time parameter `{}`",
                 name)
        .emit(dh);
      return failure::promise();
    }
    if (detail::ascii_icase_equal(name, "search_mode")) {
      if (auto const* mode = try_as<std::string>(option);
          mode and detail::ascii_icase_equal(detail::trim(*mode), "realtime")) {
        diagnostic::error("real-time Splunk searches are not supported")
          .primary(source, "`search_mode` is set to `realtime`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (detail::ascii_icase_equal(name, "index_earliest")
        or detail::ascii_icase_equal(name, "index_latest")) {
      TRY(validate_time_bound(name, option, source, dh));
    }
  }
  return {};
}

auto validate_headers(located<nova::Record> const& value,
                      diagnostic_handler& dh) -> failure_or<void> {
  auto has_authorization = false;
  for (auto const& [name, header] : value.inner) {
    if (not is<nova::String>(header) and not is<nova::Secret>(header)) {
      diagnostic::error("header values must be `string` or `secret`")
        .primary(value)
        .emit(dh);
      return failure::promise();
    }
    if (detail::ascii_icase_equal(name, "authorization")) {
      has_authorization = true;
    }
  }
  if (not has_authorization) {
    diagnostic::error("`headers` must contain an `Authorization` header")
      .primary(value)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto resolve_headers(StartupArgument<nova::Record> const& arg, OpCtx& ctx)
  -> Task<failure_or<std::vector<http::Header>>> {
  CO_TRY(auto value, co_await arg.evaluate(ctx));
  auto headers = located{std::move(value), arg.get_location()};
  CO_TRY(validate_headers(headers, ctx.dh()));
  co_return http::make_headers(headers, ctx.dh());
}

auto extract_messages(record const& envelope) -> std::vector<SplunkMessage> {
  auto result = std::vector<SplunkMessage>{};
  auto const it = envelope.find("messages");
  if (it == envelope.end()) {
    return result;
  }
  auto const* messages = try_as<list>(it->second);
  if (not messages) {
    return result;
  }
  for (auto const& message : *messages) {
    auto const* message_record = try_as<record>(message);
    if (not message_record) {
      continue;
    }
    auto parsed = SplunkMessage{};
    for (auto const& [name, value] : *message_record) {
      auto const* text = try_as<std::string>(value);
      if (not text) {
        continue;
      }
      if (name == "type") {
        parsed.type = *text;
      } else if (name == "text") {
        parsed.text = *text;
      }
    }
    if (not parsed.type.empty() or not parsed.text.empty()) {
      result.push_back(std::move(parsed));
    }
  }
  return result;
}

auto extract_messages(nova::RowView<nova::Record> envelope)
  -> std::vector<SplunkMessage> {
  auto result = std::vector<SplunkMessage>{};
  for (auto const& [name, value] : envelope) {
    if (name != "messages") {
      continue;
    }
    auto const* messages = try_as<nova::RowView<nova::List>>(value);
    if (not messages) {
      continue;
    }
    for (auto const& message : *messages) {
      auto const* fields = try_as<nova::RowView<nova::Record>>(message);
      if (not fields) {
        continue;
      }
      auto parsed = SplunkMessage{};
      for (auto const& [key, item] : *fields) {
        auto const* text = try_as<nova::RowView<nova::String>>(item);
        if (not text) {
          continue;
        }
        if (key == "type") {
          parsed.type = **text;
        } else if (key == "text") {
          parsed.text = **text;
        }
      }
      if (not parsed.type.empty() or not parsed.text.empty()) {
        result.push_back(std::move(parsed));
      }
    }
  }
  return result;
}

auto response_body_text(std::string_view text) -> std::string {
  if (auto parsed = from_json(text)) {
    if (auto const* envelope = try_as<record>(*parsed)) {
      auto messages = extract_messages(*envelope);
      if (not messages.empty()) {
        auto result = std::string{};
        for (auto const& message : messages) {
          if (not result.empty()) {
            result += "; ";
          }
          if (not message.type.empty()) {
            result += message.type;
            result += ": ";
          }
          result += message.text;
        }
        return result;
      }
    }
  }
  constexpr auto max_body_size = size_t{2048};
  text = detail::trim(text);
  if (text.size() <= max_body_size) {
    return std::string{text};
  }
  return fmt::format("{}...", text.substr(0, max_body_size));
}

auto make_export_url(std::string url) -> std::string {
  if (url.ends_with('/')) {
    url.pop_back();
  }
  url += export_path;
  return url;
}

auto make_request_target(std::string_view url, location loc,
                         diagnostic_handler& dh) -> failure_or<std::string> {
  auto parsed = boost::urls::parse_uri(url);
  if (not parsed) {
    diagnostic::error("failed to parse Splunk URL: {}",
                      parsed.error().message())
      .primary(loc)
      .emit(dh);
    return failure::promise();
  }
  return std::string{parsed->encoded_target()};
}

/// The names of the indexes in an `index` argument: a string or a list of
/// strings.
auto index_names(data const& value) -> Option<std::vector<std::string>> {
  if (auto const* name = try_as<std::string>(value)) {
    return std::vector{*name};
  }
  auto const* names = try_as<list>(value);
  if (not names) {
    return None{};
  }
  auto result = std::vector<std::string>{};
  for (auto const& name : *names) {
    auto const* text = try_as<std::string>(name);
    if (not text) {
      return None{};
    }
    result.push_back(*text);
  }
  return result;
}

/// The location of the argument that defines the search.
auto search_source(FromSplunkArgs const& args) -> location {
  if (args.search) {
    return args.search->source;
  }
  TENZIR_ASSERT(args.index);
  return args.index->source;
}

/// The search to send: the user's search or one for the indexes, with the
/// optimizer hints as trailing stages.
auto plan_search(FromSplunkArgs const& args, registry const& reg)
  -> std::string {
  auto const& hints = args.optimization;
  if (args.search) {
    return pushdown::plan_spl_search(
             pushdown::spl_search_command(args.search->inner), hints.filter,
             hints.projection, hints.limit, not args.raw, reg)
      .query;
  }
  TENZIR_ASSERT(args.index);
  auto names = index_names(args.index->inner);
  TENZIR_ASSERT(names);
  auto plan = pushdown::plan_spl_indexes(*names, hints.filter, hints.projection,
                                         hints.limit, not args.raw, reg);
  TENZIR_ASSERT(plan);
  return std::move(plan->query);
}

/// The search that computes `aggregation` in Splunk, or `None` if Splunk
/// cannot compute it as `summarize` does.
auto plan_aggregate_search(FromSplunkArgs const& args,
                           ir::OptimizeAggregation const& aggregation)
  -> Option<std::string> {
  // A pushed filter only preselects candidates, which Splunk would aggregate
  // before Tenzir evaluates the filter. Options may change how Splunk runs the
  // search in ways that the aggregation does not account for.
  if (not args.optimization.filter.empty() or args.options) {
    return None{};
  }
  auto search = std::string{};
  if (args.search) {
    search = pushdown::spl_search_command(args.search->inner);
  } else {
    auto names = args.index ? index_names(args.index->inner) : None{};
    if (not names or names->empty()) {
      return None{};
    }
    TRY(search, pushdown::spl_index_search(*names));
  }
  return pushdown::plan_spl_aggregation(search, aggregation,
                                        args.raw.is_some());
}

/// The form body of a search request. `now`, in epoch seconds, anchors the
/// relative times of the request, such as `-1h` or `now`, instead of the time
/// at which Splunk receives it.
auto make_form_body(FromSplunkArgs const& args, std::string search,
                    Option<int64_t> now = None{}) -> std::string {
  auto body = record{};
  body.emplace("search", std::move(search));
  body.emplace("earliest_time", args.earliest.inner);
  body.emplace("latest_time", args.latest.inner);
  if (now) {
    body.emplace("now", fmt::to_string(*now));
  }
  body.emplace("output_mode", std::string{"json"});
  body.emplace("preview", std::string{"false"});
  if (args.options) {
    auto const* options = try_as<record>(args.options->inner);
    TENZIR_ASSERT(options);
    for (auto const& [name, value] : *options) {
      body.emplace(name, value);
    }
  }
  return curl::escape(flatten(body));
}

// Renders a JSON parse error without the caf boilerplate, mirroring what
// `diagnostic::error(caf::error)` extracts.
auto render_parse_error(caf::error const& err) -> std::string {
  auto const& ctx = err.context();
  if (ctx.size() > 0 and ctx.match_element<std::string>(ctx.size() - 1)) {
    return ctx.get_as<std::string>(ctx.size() - 1);
  }
  return fmt::to_string(err);
}

// Messages sent from the streaming fetch task to the operator.
struct ResultBatch {
  std::vector<data> records;
};

// Periodic wake-up so that buffered results flush on the builder timeout
// even when no new response data arrives.
struct FlushTick {};

// A Splunk `messages` envelope reported a `WARN` condition.
struct StreamWarning {
  std::string type;
  std::string text;
};

// The HTTP layer retried the request.
struct RetryWarning {
  std::string message;
};

// A Splunk `messages` envelope reported an `ERROR` or `FATAL` condition.
struct SearchFailed {
  std::string text;
};

// Splunk answered with a non-success HTTP status.
struct SearchRejected {
  std::string text;
};

// The HTTP request itself failed.
struct RequestFailed {
  std::string message;
};

struct ParseFailed {
  std::string message;
  Option<std::string> detail;
};

struct StreamDone {};

using StreamMessage
  = variant<ResultBatch, FlushTick, StreamWarning, RetryWarning, SearchFailed,
            SearchRejected, RequestFailed, ParseFailed, StreamDone>;

using StreamQueue = folly::coro::BoundedQueue<StreamMessage, false, true>;

struct EventResultBatch {
  std::vector<nova::Array<nova::Record>> records;
};

using EventStreamMessage = variant<EventResultBatch, FlushTick, StreamWarning,
                                   RetryWarning, SearchFailed, SearchRejected,
                                   RequestFailed, ParseFailed, StreamDone>;
using EventStreamQueue
  = folly::coro::BoundedQueue<EventStreamMessage, false, true>;

// Streams the Splunk export response and forwards results through the queue.
// Runs detached from the operator, so it must not touch the operator or its
// diagnostic handler.
auto run_export_request(std::string url, std::string target, std::string body,
                        std::vector<http::Header> headers,
                        HttpPoolConfig config, Arc<StreamQueue> queue,
                        folly::Executor::KeepAlive<folly::IOExecutor> executor)
  -> Task<void> {
  auto success = false;
  auto stopped = false;
  auto error_body = std::string{};
  auto buffer = std::string{};
  auto records = std::vector<data>{};
  // Processes one newline-delimited response envelope, collecting `result`
  // records into `records`. Returns true to stop consuming the stream.
  auto process_line = [&](std::string_view line) -> Task<bool> {
    line = detail::trim(line);
    if (line.empty()) {
      co_return false;
    }
    auto parsed = from_json(line);
    if (not parsed) {
      co_await queue->enqueue(
        ParseFailed{"failed to parse Splunk response as JSON",
                    render_parse_error(parsed.error())});
      co_return true;
    }
    auto* envelope = try_as<record>(*parsed);
    if (not envelope) {
      co_await queue->enqueue(
        ParseFailed{"expected Splunk response to be a JSON object", {}});
      co_return true;
    }
    for (auto const& message : extract_messages(*envelope)) {
      auto text = message.text.empty() ? std::string{"no details provided"}
                                       : message.text;
      if (detail::ascii_icase_equal(message.type, "ERROR")
          or detail::ascii_icase_equal(message.type, "FATAL")) {
        co_await queue->enqueue(SearchFailed{std::move(text)});
        co_return true;
      }
      auto type = message.type.empty() ? std::string{"message"} : message.type;
      if (detail::ascii_icase_equal(message.type, "WARN")) {
        co_await queue->enqueue(
          StreamWarning{std::move(type), std::move(text)});
        continue;
      }
      TENZIR_DEBUG("from_splunk: search returned {}: {}", type, text);
    }
    auto const it = envelope->find("result");
    if (it == envelope->end() or is<caf::none_t>(it->second)) {
      co_return false;
    }
    if (is<record>(it->second)) {
      records.push_back(std::move(it->second));
    }
    co_return false;
  };
  auto callbacks = HttpStreamCallbacks{};
  callbacks.on_headers = [&](http::Response const& response) {
    success = response.is_status_success();
  };
  callbacks.on_body = [&](std::string chunk) -> Task<bool> {
    if (not success) {
      constexpr auto max_error_body_size = size_t{65536};
      if (error_body.size() < max_error_body_size) {
        error_body += chunk;
      }
      co_return false;
    }
    buffer += chunk;
    auto pos = size_t{};
    while ((pos = buffer.find('\n')) != std::string::npos) {
      auto const line = std::string_view{buffer.data(), pos};
      if (co_await process_line(line)) {
        stopped = true;
        co_return true;
      }
      buffer.erase(0, pos + 1);
    }
    if (not records.empty()) {
      co_await queue->enqueue(ResultBatch{std::move(records)});
      records.clear();
    }
    co_return false;
  };
  auto pool = HttpPool::make(std::move(executor), url, std::move(config));
  auto result
    = co_await pool->stream_post(std::move(target), std::move(body),
                                 std::move(headers), std::move(callbacks));
  if (stopped) {
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  if (result.is_err()) {
    co_await queue->enqueue(RequestFailed{std::move(result).unwrap_err()});
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  auto response = std::move(result).unwrap();
  if (not response.is_status_success()) {
    auto message = response_body_text(error_body);
    if (message.empty()) {
      message = "HTTP request failed without a response body";
    }
    co_await queue->enqueue(SearchRejected{std::move(message)});
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  if (not buffer.empty()) {
    if (co_await process_line(buffer)) {
      co_await queue->enqueue(StreamDone{});
      co_return;
    }
  }
  if (not records.empty()) {
    co_await queue->enqueue(ResultBatch{std::move(records)});
  }
  co_await queue->enqueue(StreamDone{});
}

class FromSplunk final : public Operator<void, table_slice> {
public:
  explicit FromSplunk(FromSplunkArgs args)
    : args_{std::move(args)}, hints_{args_.optimization.limit} {
    // Only `FromSplunkEvents` takes over aggregations.
    TENZIR_ASSERT(not args_.optimization.aggregation);
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await hints_.prepare(args_.optimization.filter, ctx)) {
      done_ = true;
      co_return;
    }
    events_read_
      = ctx.make_counter(MetricsLabel{"operator", "from_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto headers = co_await resolve_headers(args_.headers, ctx);
    if (not headers) {
      done_ = true;
      co_return;
    }
    auto resolved_headers = std::move(*headers);
    auto url = make_export_url(args_.url.inner);
    auto const request_timeout
      = args_.timeout ? std::chrono::duration_cast<std::chrono::milliseconds>(
                          args_.timeout->inner)
                      : std::chrono::milliseconds{http::default_timeout};
    auto config = http::make_http_pool_config(args_.tls, url, args_.url.source,
                                              ctx.dh(), request_timeout,
                                              ctx.actor_system().config());
    if (not config) {
      done_ = true;
      co_return;
    }
    if (args_.connection_timeout) {
      config->connection_timeout
        = std::chrono::duration_cast<std::chrono::milliseconds>(
          args_.connection_timeout->inner);
    }
    config->max_retry_count
      = args_.max_retry_count
          ? detail::narrow<uint32_t>(args_.max_retry_count->inner)
          : http::default_max_retry_count;
    config->retry_delay
      = args_.retry_delay
          ? std::chrono::duration_cast<std::chrono::milliseconds>(
              args_.retry_delay->inner)
          : std::chrono::milliseconds{http::default_retry_delay};
    auto target = make_request_target(url, args_.url.source, ctx.dh());
    if (not target) {
      done_ = true;
      co_return;
    }
    if (not http::find(resolved_headers, "content-type")) {
      resolved_headers.emplace_back("Content-Type",
                                    "application/x-www-form-urlencoded");
    }
    if (not http::find(resolved_headers, "accept")) {
      resolved_headers.emplace_back("Accept", "application/json");
    }
    auto queue = Arc<StreamQueue>{std::in_place, 64};
    queue_ = queue;
    config->on_retry = [queue](std::string_view message) mutable {
      std::ignore = queue->try_enqueue(RetryWarning{std::string{message}});
    };
    url_ = url;
    builder_ = std::make_unique<multi_series_builder>(
      multi_series_builder::options{
        .settings = {.default_schema_name = "tenzir.splunk"},
      },
      ctx.dh());
    // Wake the operator periodically so that buffered results flush on the
    // builder timeout even when the response stream goes quiet.
    ctx.spawn_task(
      [queue, cancel = cancel_.getToken()]() mutable -> Task<void> {
        auto token = folly::cancellation_token_merge(
          co_await folly::coro::co_current_cancellation_token, cancel);
        co_await folly::coro::co_withCancellation(
          token, folly::coro::co_invoke(
                   [queue = std::move(queue)]() mutable -> Task<void> {
                     while (true) {
                       co_await sleep_for(std::chrono::milliseconds{250});
                       std::ignore = queue->try_enqueue(FlushTick{});
                     }
                   }));
      });
    search_ = plan_search(args_, ctx.reg());
    TENZIR_DEBUG("from_splunk: submitting streaming search without "
                 "pagination: {}",
                 search_);
    ctx.spawn_task([url = std::move(url), target = std::move(*target),
                    body = make_form_body(args_, search_),
                    headers = std::move(resolved_headers),
                    config = std::move(*config), queue = std::move(queue),
                    executor = ctx.io_executor(),
                    cancel = cancel_.getToken()]() mutable -> Task<void> {
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token, cancel);
      co_await folly::coro::co_withCancellation(
        token,
        run_export_request(std::move(url), std::move(target), std::move(body),
                           std::move(headers), std::move(config),
                           std::move(queue), std::move(executor)));
    });
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_ or not queue_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return Any{co_await (*queue_)->dequeue()};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    co_await co_match(
      std::move(result).as<StreamMessage>(),
      [&](ResultBatch msg) -> Task<void> {
        TENZIR_ASSERT(builder_);
        for (auto& record : msg.records) {
          builder_->data(std::move(record));
        }
        co_await flush(push, ctx);
      },
      [&](FlushTick) -> Task<void> {
        co_await flush(push, ctx);
      },
      [&](StreamWarning msg) -> Task<void> {
        diagnostic::warning("Splunk search returned {}: {}", msg.type, msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        co_return;
      },
      [&](RetryWarning msg) -> Task<void> {
        diagnostic::warning("{}", msg.message)
          .primary(args_.url.source)
          .emit(ctx);
        co_return;
      },
      [&](SearchFailed msg) -> Task<void> {
        diagnostic::error("Splunk search failed: {}", msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](SearchRejected msg) -> Task<void> {
        diagnostic::error("Splunk rejected the search: {}", msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](RequestFailed msg) -> Task<void> {
        diagnostic::error("HTTP request to `{}` failed: {}", url_, msg.message)
          .primary(args_.url.source)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](ParseFailed msg) -> Task<void> {
        auto builder
          = diagnostic::error("{}", msg.message).primary(args_.url.source);
        if (msg.detail) {
          builder = std::move(builder).note("{}", *msg.detail);
        }
        std::move(builder).emit(ctx);
        done_ = true;
        co_return;
      },
      [&](StreamDone) -> Task<void> {
        if (builder_) {
          for (auto&& slice : builder_->finalize_as_table_slice()) {
            co_await emit(std::move(slice), push, ctx);
          }
        }
        done_ = true;
        co_return;
      });
  }

  auto flush(Push<table_slice>& push, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(builder_);
    // The 250ms flush ticks drive timeout-based yields, so the `wait_for`
    // hint in the result does not need separate scheduling.
    auto ready = builder_->yield_ready_as_table_slice();
    for (auto&& slice : ready.slices) {
      co_await emit(std::move(slice), push, ctx);
    }
  }

  // Applies the local hints and stops the search once the limit is reached.
  auto emit(table_slice slice, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
    if (done_) {
      co_return;
    }
    events_read_.add(slice.rows());
    slice = hints_.apply(std::move(slice), ctx);
    if (slice.rows() > 0) {
      co_await push(std::move(slice));
    }
    if (hints_.exhausted()) {
      cancel_.requestCancellation();
      done_ = true;
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    cancel_.requestCancellation();
    if (queue_) {
      std::ignore = (*queue_)->try_enqueue(StreamDone{});
    }
    done_ = true;
    co_return;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  FromSplunkArgs args_;
  pushdown::LocalHints<table_slice> hints_;
  std::string search_;
  std::string url_;
  mutable Option<Arc<StreamQueue>> queue_;
  std::unique_ptr<multi_series_builder> builder_;
  folly::CancellationSource cancel_;
  MetricsCounter events_read_;
  bool done_ = false;
};

auto run_export_request_events(
  std::string url, std::string target, std::string body,
  std::vector<http::Header> headers, HttpPoolConfig config,
  Arc<EventStreamQueue> queue,
  folly::Executor::KeepAlive<folly::IOExecutor> executor, bool raw)
  -> Task<void> {
  auto success = false;
  auto stopped = false;
  auto error_body = std::string{};
  auto buffer = std::string{};
  auto records = std::vector<nova::Array<nova::Record>>{};
  auto parser_dh = collecting_diagnostic_handler{};
  auto settings = nova::JsonParser::Settings{};
  // Splunk returns every field value as a string. Infer numbers, booleans,
  // and the other types that strings spell inside results, so that TQL
  // compares values as Splunk's `search` does. Envelope fields, such as
  // message texts, stay strings.
  if (raw) {
    settings.builder.raw = true;
  } else {
    settings.builder.infer_unparsed_under = "result";
  }
  settings.decoding.first_duplicate_wins = true;
  settings.decoding.reject_oversized_integers = true;
  auto parser = nova::JsonParser::make(std::move(settings), parser_dh);
  TENZIR_ASSERT(parser);
  // Processes one newline-delimited response envelope, collecting `result`
  // records into `records`. Returns true to stop consuming the stream.
  auto process_line = [&](std::string_view line) -> Task<bool> {
    line = detail::trim(line);
    if (line.empty()) {
      co_return false;
    }
    // Strict whole-frame decoding: never expose a partial result.
    auto parsed = parser->parse_document_value(line);
    if (not parsed) {
      auto diagnostics = std::move(parser_dh).collect();
      auto detail
        = diagnostics.empty()
            ? Option<std::string>{None{}}
            : Option<std::string>{std::move(diagnostics.front().message)};
      co_await queue->enqueue(ParseFailed{
        "failed to parse Splunk response as JSON", std::move(detail)});
      co_return true;
    }
    auto envelope = parsed->get_alternative<nova::Record>();
    if (not envelope) {
      co_await queue->enqueue(
        ParseFailed{"expected Splunk response to be a JSON object", {}});
      co_return true;
    }
    for (auto const& message : extract_messages(envelope->data.get(0))) {
      auto text = message.text.empty() ? std::string{"no details provided"}
                                       : message.text;
      if (detail::ascii_icase_equal(message.type, "ERROR")
          or detail::ascii_icase_equal(message.type, "FATAL")) {
        co_await queue->enqueue(SearchFailed{std::move(text)});
        co_return true;
      }
      auto type = message.type.empty() ? std::string{"message"} : message.type;
      if (detail::ascii_icase_equal(message.type, "WARN")) {
        co_await queue->enqueue(
          StreamWarning{std::move(type), std::move(text)});
        continue;
      }
      TENZIR_DEBUG("from_splunk: search returned {}: {}", type, text);
    }
    auto result = envelope->data.field("result");
    if (result) {
      if (auto values
          = std::move(result->data).get_alternative<nova::Record>()) {
        records.push_back(std::move(values->data));
      }
    }
    co_return false;
  };
  auto callbacks = HttpStreamCallbacks{};
  callbacks.on_headers = [&](http::Response const& response) {
    success = response.is_status_success();
  };
  callbacks.on_body = [&](std::string chunk) -> Task<bool> {
    if (not success) {
      constexpr auto max_error_body_size = size_t{65536};
      if (error_body.size() < max_error_body_size) {
        error_body += chunk;
      }
      co_return false;
    }
    buffer += chunk;
    auto pos = size_t{};
    while ((pos = buffer.find('\n')) != std::string::npos) {
      auto const line = std::string_view{buffer.data(), pos};
      if (co_await process_line(line)) {
        stopped = true;
        co_return true;
      }
      buffer.erase(0, pos + 1);
    }
    if (not records.empty()) {
      co_await queue->enqueue(EventResultBatch{std::move(records)});
      records.clear();
    }
    co_return false;
  };
  auto pool = HttpPool::make(std::move(executor), url, std::move(config));
  auto result
    = co_await pool->stream_post(std::move(target), std::move(body),
                                 std::move(headers), std::move(callbacks));
  if (stopped) {
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  if (result.is_err()) {
    co_await queue->enqueue(RequestFailed{std::move(result).unwrap_err()});
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  auto response = std::move(result).unwrap();
  if (not response.is_status_success()) {
    auto message = response_body_text(error_body);
    if (message.empty()) {
      message = "HTTP request failed without a response body";
    }
    co_await queue->enqueue(SearchRejected{std::move(message)});
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  if (not buffer.empty()) {
    if (co_await process_line(buffer)) {
      co_await queue->enqueue(StreamDone{});
      co_return;
    }
  }
  if (not records.empty()) {
    co_await queue->enqueue(EventResultBatch{std::move(records)});
  }
  co_await queue->enqueue(StreamDone{});
}

class FromSplunkEvents final : public Operator<void, nova::Events> {
public:
  explicit FromSplunkEvents(FromSplunkArgs args)
    : args_{std::move(args)}, hints_{args_.optimization.limit} {
    hints_.set_projection(args_.optimization.projection);
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await hints_.prepare(args_.optimization.filter, ctx)) {
      done_ = true;
      co_return;
    }
    events_read_
      = ctx.make_counter(MetricsLabel{"operator", "from_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto headers = co_await resolve_headers(args_.headers, ctx);
    if (not headers) {
      done_ = true;
      co_return;
    }
    auto resolved_headers = std::move(*headers);
    auto url = make_export_url(args_.url.inner);
    auto const request_timeout
      = args_.timeout ? std::chrono::duration_cast<std::chrono::milliseconds>(
                          args_.timeout->inner)
                      : std::chrono::milliseconds{http::default_timeout};
    auto config = http::make_http_pool_config(args_.tls, url, args_.url.source,
                                              ctx.dh(), request_timeout,
                                              ctx.actor_system().config());
    if (not config) {
      done_ = true;
      co_return;
    }
    if (args_.connection_timeout) {
      config->connection_timeout
        = std::chrono::duration_cast<std::chrono::milliseconds>(
          args_.connection_timeout->inner);
    }
    config->max_retry_count
      = args_.max_retry_count
          ? detail::narrow<uint32_t>(args_.max_retry_count->inner)
          : http::default_max_retry_count;
    config->retry_delay
      = args_.retry_delay
          ? std::chrono::duration_cast<std::chrono::milliseconds>(
              args_.retry_delay->inner)
          : std::chrono::milliseconds{http::default_retry_delay};
    auto target = make_request_target(url, args_.url.source, ctx.dh());
    if (not target) {
      done_ = true;
      co_return;
    }
    if (not http::find(resolved_headers, "content-type")) {
      resolved_headers.emplace_back("Content-Type",
                                    "application/x-www-form-urlencoded");
    }
    if (not http::find(resolved_headers, "accept")) {
      resolved_headers.emplace_back("Accept", "application/json");
    }
    auto queue = Arc<EventStreamQueue>{std::in_place, 64};
    queue_ = queue;
    config->on_retry = [queue](std::string_view message) mutable {
      std::ignore = queue->try_enqueue(RetryWarning{std::string{message}});
    };
    url_ = std::move(url);
    target_ = std::move(*target);
    headers_ = std::move(resolved_headers);
    config_ = std::move(*config);
    if (auto const& aggregation = args_.optimization.aggregation) {
      if (auto search = plan_aggregate_search(args_, *aggregation)) {
        aggregating_remotely_ = true;
        // If the result of the aggregation might differ from `summarize`, the
        // operator sends the search again without it. Both requests resolve
        // relative times, such as `-1h`, against the same current time, as a
        // single request would. The `options` that could set `now` themselves
        // keep the aggregation local.
        now_ = std::chrono::floor<std::chrono::seconds>(
                 time::clock::now().time_since_epoch())
                 .count();
        launch(std::move(*search), ctx);
        co_return;
      }
      if (not co_await aggregate_locally(ctx)) {
        done_ = true;
        co_return;
      }
    }
    launch(plan_search(args_, ctx.reg()), ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_ or not queue_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not next_flush_) {
      co_return Any{co_await (*queue_)->dequeue()};
    }
    // Keep a concurrently completed dequeue even if the flush timer also fires.
    auto [message, timer] = co_await folly::coro::collectAnyNoDiscard(
      (*queue_)->dequeue(), sleep_until(*next_flush_));
    if (message.hasValue()) {
      co_return Any{std::move(message).value()};
    }
    timer.value();
    co_return Any{EventStreamMessage{FlushTick{}}};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    co_await co_match(
      std::move(result).as<EventStreamMessage>(),
      [&](EventResultBatch msg) -> Task<void> {
        if (aggregating_remotely_) {
          std::ranges::move(msg.records, std::back_inserter(aggregation_rows_));
          co_return;
        }
        for (auto const& value : msg.records) {
          if (not next_flush_) {
            next_flush_ = std::chrono::steady_clock::now()
                          + defaults::import::batch_timeout;
          }
          auto row = builder_.record();
          for (auto const& [name, field] : value.get(0)) {
            nova::append_row(row.field(name), field);
          }
          if (builder_.length() >= detail::narrow<nova::storage::Index>(
                defaults::import::table_slice_size)) {
            co_await flush(push, ctx);
          }
        }
      },
      [&](FlushTick) -> Task<void> {
        co_await flush(push, ctx);
      },
      [&](StreamWarning msg) -> Task<void> {
        // A warning may tell that the result of the aggregation is incomplete.
        if (aggregating_remotely_) {
          reject_aggregation(
            fmt::format("Splunk search returned {}: {}", msg.type, msg.text));
          co_return;
        }
        diagnostic::warning("Splunk search returned {}: {}", msg.type, msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        co_return;
      },
      [&](RetryWarning msg) -> Task<void> {
        diagnostic::warning("{}", msg.message)
          .primary(args_.url.source)
          .emit(ctx);
        co_return;
      },
      [&](SearchFailed msg) -> Task<void> {
        if (aggregating_remotely_) {
          reject_aggregation(fmt::format("Splunk search failed: {}", msg.text));
          co_return;
        }
        diagnostic::error("Splunk search failed: {}", msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](SearchRejected msg) -> Task<void> {
        if (aggregating_remotely_) {
          reject_aggregation(
            fmt::format("Splunk rejected the search: {}", msg.text));
          co_return;
        }
        diagnostic::error("Splunk rejected the search: {}", msg.text)
          .primary(search_source(args_))
          .note("search: {}", search_)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](RequestFailed msg) -> Task<void> {
        if (aggregating_remotely_) {
          reject_aggregation(
            fmt::format("HTTP request failed: {}", msg.message));
          co_return;
        }
        diagnostic::error("HTTP request to `{}` failed: {}", url_, msg.message)
          .primary(args_.url.source)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](ParseFailed msg) -> Task<void> {
        if (aggregating_remotely_) {
          reject_aggregation(std::move(msg.message));
          co_return;
        }
        auto builder
          = diagnostic::error("{}", msg.message).primary(args_.url.source);
        if (msg.detail) {
          builder = std::move(builder).note("{}", *msg.detail);
        }
        std::move(builder).emit(ctx);
        done_ = true;
        co_return;
      },
      [&](StreamDone) -> Task<void> {
        if (aggregating_remotely_) {
          co_await finish_aggregation(push, ctx);
          co_return;
        }
        co_await flush(push, ctx);
        done_ = true;
        co_await close_sub(ctx);
        co_return;
      });
    // Any continuously ready message kind can starve the latency deadline.
    // Process the dequeued message first, but never flush after a fatal error.
    if (not done_ and next_flush_
        and std::chrono::steady_clock::now() >= *next_flush_) {
      co_await flush(push, ctx);
    }
  }

  auto finish_sub(SubKeyView, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    aggregating_locally_ = false;
    co_return;
  }

  auto finish_sub(SubKeyView, failure, Push<nova::Events>&, OpCtx&)
    -> Task<void> override {
    // The subpipeline reported its error, so the search cannot complete.
    aggregating_locally_ = false;
    cancel_.requestCancellation();
    done_ = true;
    co_return;
  }

  auto flush(Push<nova::Events>& push, OpCtx& ctx) -> Task<void> {
    auto length = builder_.length();
    if (done_ or length == 0) {
      co_return;
    }
    next_flush_ = None{};
    // finish() consumes the builder; start subsequent batches with fresh state.
    auto data
      = std::exchange(builder_, nova::ArrayBuilder<nova::Record>{}).finish();
    auto events
      = nova::Events{std::move(data), nova::storage::BitMap{length, true},
                     nova::Events::Meta::make_empty(length, "tenzir.splunk")};
    events_read_.add(events.active_count());
    events = hints_.apply(std::move(events), ctx);
    if (events.mask.any()) {
      if (aggregating_locally_) {
        if (auto sub = ctx.get_sub(aggregation_sub_key)) {
          // The subpipeline closes its input only when it fails, which it
          // reports itself.
          std::ignore = co_await as<SubHandle<nova::Events>>(*sub).push(
            std::move(events));
        }
      } else {
        co_await push(std::move(events));
      }
    }
    // Stop the search once the limit is reached.
    if (hints_.exhausted()) {
      cancel_.requestCancellation();
      done_ = true;
    }
  }

  auto state() -> OperatorState override {
    if (aggregating_locally_) {
      return OperatorState::normal;
    }
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    cancel_.requestCancellation();
    if (queue_) {
      std::ignore = (*queue_)->try_enqueue(StreamDone{});
    }
    done_ = true;
    co_await close_sub(ctx);
  }

  auto snapshot(Serde&) -> void override {
    // Restarting a streaming search would duplicate already committed results.
    diagnostic::error("from_splunk does not support checkpoints yet")
      .primary(args_.operator_location)
      .throw_();
  }

private:
  /// Sends `search` and streams its results into the queue.
  auto launch(std::string search, OpCtx& ctx) -> void {
    TENZIR_ASSERT(queue_ and config_);
    search_ = std::move(search);
    TENZIR_DEBUG("from_splunk: submitting streaming search without "
                 "pagination{}: {}",
                 now_ ? fmt::format(" at {}", *now_) : std::string{}, search_);
    // The decoder of an aggregation types the values of its strings itself.
    auto raw = aggregating_remotely_ or args_.raw.is_some();
    ctx.spawn_task([url = url_, target = target_,
                    body = make_form_body(args_, search_, now_),
                    headers = headers_, config = *config_, queue = *queue_,
                    executor = ctx.io_executor(), raw,
                    cancel = cancel_.getToken()]() mutable -> Task<void> {
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token, cancel);
      co_await folly::coro::co_withCancellation(
        token, run_export_request_events(std::move(url), std::move(target),
                                         std::move(body), std::move(headers),
                                         std::move(config), std::move(queue),
                                         std::move(executor), raw));
    });
  }

  /// Spawns the subpipeline that aggregates the results of the search.
  auto aggregate_locally(OpCtx& ctx) -> Task<bool> {
    TENZIR_ASSERT(args_.optimization.aggregation);
    auto const& aggregation = *args_.optimization.aggregation;
    if (not pushdown::can_aggregate_locally(aggregation, ctx)) {
      co_return false;
    }
    auto sub = co_await ctx.plan_and_spawn_sub<nova::Events>(
      aggregation_sub_key, aggregation.fallback);
    if (not sub) {
      co_return false;
    }
    aggregating_locally_ = true;
    co_return true;
  }

  /// Ends the input of the subpipeline that aggregates locally, which then
  /// emits the aggregation and finishes.
  auto close_sub(OpCtx& ctx) -> Task<void> {
    if (not aggregating_locally_ or sub_closed_) {
      co_return;
    }
    sub_closed_ = true;
    if (auto sub = ctx.get_sub(aggregation_sub_key)) {
      co_await as<SubHandle<nova::Events>>(*sub).close();
    }
  }

  /// Records why the result of the aggregation in Splunk may be incomplete.
  /// The search still runs to its end.
  auto reject_aggregation(std::string reason) -> void {
    if (not aggregation_error_) {
      aggregation_error_ = std::move(reason);
    }
  }

  /// Emits the result of the aggregation in Splunk, or sends the search
  /// without it and aggregates its results locally if the result might differ
  /// from what `summarize` computes.
  auto finish_aggregation(Push<nova::Events>& push, OpCtx& ctx) -> Task<void> {
    aggregating_remotely_ = false;
    auto rows = std::exchange(aggregation_rows_, {});
    auto const& aggregation = *args_.optimization.aggregation;
    auto dh = collecting_diagnostic_handler{};
    auto result = aggregation_error_
                    ? Result<std::vector<nova::Events>, std::string>{Err{
                        std::move(*aggregation_error_)}}
                    : pushdown::decode_spl_aggregation(rows, aggregation,
                                                       args_.raw.is_some(), dh);
    if (result.is_err()) {
      TENZIR_DEBUG("from_splunk aggregates locally: {}", result.unwrap_err());
      if (not co_await aggregate_locally(ctx)) {
        done_ = true;
        co_return;
      }
      launch(plan_search(args_, ctx.reg()), ctx);
      co_return;
    }
    std::move(dh).forward_to(ctx.dh());
    for (auto& events : std::move(result).unwrap()) {
      events_read_.add(events.active_count());
      co_await push(std::move(events));
    }
    done_ = true;
  }

  FromSplunkArgs args_;
  pushdown::LocalHints<nova::Events> hints_;
  std::string search_;
  std::string url_;
  std::string target_;
  std::vector<http::Header> headers_;
  Option<HttpPoolConfig> config_;
  mutable Option<Arc<EventStreamQueue>> queue_;
  nova::ArrayBuilder<nova::Record> builder_;
  Option<std::chrono::steady_clock::time_point> next_flush_;
  /// The current time of every request, in epoch seconds, if the operator may
  /// send the search twice.
  Option<int64_t> now_;
  /// Whether the current search computes the aggregation in Splunk.
  bool aggregating_remotely_ = false;
  /// Why the result of the aggregation in Splunk may be incomplete.
  Option<std::string> aggregation_error_;
  /// The rows of the result of the aggregation in Splunk.
  std::vector<nova::Array<nova::Record>> aggregation_rows_;
  /// Whether the results of the search go to the subpipeline that aggregates
  /// them, until it finishes.
  bool aggregating_locally_ = false;
  /// Whether the input of that subpipeline ended.
  bool sub_closed_ = false;
  folly::CancellationSource cancel_;
  MetricsCounter events_read_;
  bool done_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_splunk";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromSplunkArgs, FromSplunk, FromSplunkEvents>{};
    auto url = d.positional("url", &FromSplunkArgs::url);
    auto search = d.named("search", &FromSplunkArgs::search, "string");
    auto index
      = d.named("index", &FromSplunkArgs::index, "string|list<string>");
    auto earliest
      = d.named("earliest", &FromSplunkArgs::earliest, "string|time");
    auto latest = d.named("latest", &FromSplunkArgs::latest, "string|time");
    auto options = d.named("options", &FromSplunkArgs::options, "record");
    d.named("headers", &FromSplunkArgs::headers);
    auto tls = d.named("tls", &FromSplunkArgs::tls, "record");
    auto timeout = d.named("timeout", &FromSplunkArgs::timeout);
    auto connection_timeout
      = d.named("connection_timeout", &FromSplunkArgs::connection_timeout);
    auto max_retry_count
      = d.named("max_retry_count", &FromSplunkArgs::max_retry_count);
    auto retry_delay = d.named("retry_delay", &FromSplunkArgs::retry_delay);
    d.named("raw", &FromSplunkArgs::raw);
    d.operator_location(&FromSplunkArgs::operator_location);
    d.optimization(&FromSplunkArgs::optimization);
    // Splunk can compute a summary of the results of a search in `stats`
    // stages, unless a filter applies to them, since a pushed filter only
    // preselects candidates. The operator checks the result, and aggregates
    // locally if it might differ from `summarize`. Only the Nova operator
    // implements the aggregation.
    d.aggregation([](FromSplunkArgs const& args,
                     ir::OptimizeAggregation const& aggregation) -> bool {
      return nova_enabled()
             and plan_aggregate_search(args, aggregation).is_some();
    });
    d.validate([=](DescribeCtx& ctx) -> Empty {
      // Fail fast so that one invalid argument yields one diagnostic.
      auto run = [&]() -> failure_or<void> {
        if (auto value = ctx.get(url)) {
          TRY(check_non_empty("url", *value, ctx));
        }
        auto search_location = ctx.get_location(search);
        auto index_location = ctx.get_location(index);
        if (search_location and index_location) {
          diagnostic::error("`search` and `index` are mutually exclusive")
            .primary(*search_location)
            .primary(*index_location)
            .emit(ctx);
          return failure::promise();
        }
        if (not search_location and not index_location) {
          diagnostic::error("either `search` or `index` is required")
            .primary(ctx.operator_location())
            .hint("use `index` to read events, or `search` to run SPL")
            .emit(ctx);
          return failure::promise();
        }
        if (auto value = ctx.get(search)) {
          TRY(check_non_empty("search",
                              located{std::string{detail::trim(value->inner)},
                                      value->source},
                              ctx));
        }
        if (auto value = ctx.get(index)) {
          auto names = index_names(value->inner);
          if (not names or names->empty()
              or not pushdown::spl_index_search(*names)) {
            diagnostic::error(
              "`index` must be a non-empty index name or a non-empty list of "
              "them")
              .primary(*value)
              .emit(ctx);
            return failure::promise();
          }
        }
        if (auto value = ctx.get(earliest)) {
          TRY(
            validate_time_bound("earliest", value->inner, value->source, ctx));
        }
        if (auto value = ctx.get(latest)) {
          TRY(validate_time_bound("latest", value->inner, value->source, ctx));
        }
        if (auto value = ctx.get(options)) {
          TRY(validate_options(*value, ctx));
        }
        if (auto value = ctx.get(tls)) {
          auto tls_opts = tls_options{*value, {.is_server = false}};
          if (auto url_value = ctx.get(url)) {
            TRY(tls_opts.validate(*url_value, ctx));
          } else {
            TRY(tls_opts.validate(ctx));
          }
        }
        // Validate timeout/retry arguments like the other HTTP operators.
        auto check_non_negative
          = [&](std::string_view name,
                Option<located<duration>> value) -> failure_or<void> {
          if (value and value->inner < duration::zero()) {
            diagnostic::error("`{}` must be a non-negative duration", name)
              .primary(value->source)
              .emit(ctx);
            return failure::promise();
          }
          return {};
        };
        TRY(check_non_negative("timeout", ctx.get(timeout)));
        TRY(check_non_negative("connection_timeout",
                               ctx.get(connection_timeout)));
        TRY(check_non_negative("retry_delay", ctx.get(retry_delay)));
        if (auto value = ctx.get(max_retry_count)) {
          if (value->inner > std::numeric_limits<uint32_t>::max()) {
            diagnostic::error("`max_retry_count` must be <= {}",
                              std::numeric_limits<uint32_t>::max())
              .primary(value->source)
              .emit(ctx);
            return failure::promise();
          }
        }
        return {};
      };
      std::ignore = run();
      return {};
    });
    // The optimization inputs above absorb the downstream filters, limit, and
    // projection; only the remaining, empty request reaches this callback.
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_splunk::Plugin)
