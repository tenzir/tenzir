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
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/http.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/location.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"
#include "tenzir/variant.hpp"

#include <boost/url/parse.hpp>
#include <caf/error.hpp>
#include <caf/message.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/WithCancellation.h>

#include <chrono>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_splunk {

namespace {

constexpr auto export_path
  = std::string_view{"/services/search/v2/jobs/export"};

struct FromSplunkArgs {
  located<std::string> url;
  located<std::string> search;
  located<data> earliest;
  located<data> latest;
  Option<located<data>> options;
  located<data> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  location operator_location = location::unknown;
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

auto validate_headers(located<data> const& value, diagnostic_handler& dh)
  -> failure_or<void> {
  auto const* headers = try_as<record>(value.inner);
  if (not headers) {
    diagnostic::error("`headers` must be a record").primary(value).emit(dh);
    return failure::promise();
  }
  for (auto const& [name, _] : *headers) {
    if (detail::ascii_icase_equal(name, "authorization")) {
      return {};
    }
  }
  diagnostic::error("`headers` must contain an `Authorization` header")
    .primary(value)
    .emit(dh);
  return failure::promise();
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

auto make_form_body(FromSplunkArgs const& args) -> std::string {
  auto body = record{};
  body.emplace("search", args.search.inner);
  body.emplace("earliest_time", args.earliest.inner);
  body.emplace("latest_time", args.latest.inner);
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
struct StreamSlice {
  table_slice slice;
};

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
  = variant<StreamSlice, StreamWarning, RetryWarning, SearchFailed,
            SearchRejected, RequestFailed, ParseFailed, StreamDone>;

using StreamQueue = folly::coro::BoundedQueue<StreamMessage, false, true>;

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
  auto builder_dh = null_diagnostic_handler{};
  auto builder = multi_series_builder{
    multi_series_builder::options{
      .settings = {.default_schema_name = "tenzir.splunk"},
    },
    builder_dh};
  // Processes one newline-delimited response envelope. Returns true to stop
  // consuming the stream.
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
      builder.data(std::move(it->second));
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
    for (auto&& slice : builder.yield_ready_as_table_slice()) {
      co_await queue->enqueue(StreamSlice{std::move(slice)});
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
  for (auto&& slice : builder.finalize_as_table_slice()) {
    co_await queue->enqueue(StreamSlice{std::move(slice)});
  }
  co_await queue->enqueue(StreamDone{});
}

class FromSplunk final : public Operator<void, table_slice> {
public:
  explicit FromSplunk(FromSplunkArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    events_read_
      = ctx.make_counter(MetricsLabel{"operator", "from_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto resolved_headers = std::vector<http::Header>{};
    auto header_requests = http::make_header_secret_requests(
      Option<located<data>>{args_.headers}, resolved_headers, ctx.dh());
    if (auto result = co_await ctx.resolve_secrets(std::move(header_requests));
        result.is_error()) {
      done_ = true;
      co_return;
    }
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
    TENZIR_DEBUG("from_splunk: submitting streaming search without "
                 "pagination: {}",
                 args_.search.inner);
    ctx.spawn_task([url = std::move(url), target = std::move(*target),
                    body = make_form_body(args_),
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
      [&](StreamSlice msg) -> Task<void> {
        auto const rows = msg.slice.rows();
        co_await push(std::move(msg.slice));
        events_read_.add(rows);
      },
      [&](StreamWarning msg) -> Task<void> {
        diagnostic::warning("Splunk search returned {}: {}", msg.type, msg.text)
          .primary(args_.search.source)
          .note("search: {}", args_.search.inner)
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
          .primary(args_.search.source)
          .note("search: {}", args_.search.inner)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](SearchRejected msg) -> Task<void> {
        diagnostic::error("Splunk rejected the search: {}", msg.text)
          .primary(args_.search.source)
          .note("search: {}", args_.search.inner)
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
        done_ = true;
        co_return;
      });
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
  std::string url_;
  mutable Option<Arc<StreamQueue>> queue_;
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
    auto d = Describer<FromSplunkArgs, FromSplunk>{};
    auto url = d.positional("url", &FromSplunkArgs::url);
    auto search = d.named("search", &FromSplunkArgs::search, "string");
    auto earliest
      = d.named("earliest", &FromSplunkArgs::earliest, "string|time");
    auto latest = d.named("latest", &FromSplunkArgs::latest, "string|time");
    auto options = d.named("options", &FromSplunkArgs::options, "record");
    auto headers = d.named("headers", &FromSplunkArgs::headers, "record");
    auto tls = d.named("tls", &FromSplunkArgs::tls, "record");
    auto timeout = d.named("timeout", &FromSplunkArgs::timeout);
    auto connection_timeout
      = d.named("connection_timeout", &FromSplunkArgs::connection_timeout);
    auto max_retry_count
      = d.named("max_retry_count", &FromSplunkArgs::max_retry_count);
    auto retry_delay = d.named("retry_delay", &FromSplunkArgs::retry_delay);
    d.operator_location(&FromSplunkArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      // Fail fast so that one invalid argument yields one diagnostic.
      auto run = [&]() -> failure_or<void> {
        if (auto value = ctx.get(url)) {
          TRY(check_non_empty("url", *value, ctx));
        }
        if (auto value = ctx.get(search)) {
          TRY(check_non_empty("search", *value, ctx));
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
        if (auto value = ctx.get(headers)) {
          TRY(validate_headers(*value, ctx));
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
                std::optional<located<duration>> value) -> failure_or<void> {
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_splunk::Plugin)
