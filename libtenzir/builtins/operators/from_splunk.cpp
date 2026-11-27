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
#include "tenzir/checked_math.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/concept/parseable/numeric/real.hpp"
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
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/json_parser.hpp"
#include "tenzir/operator/optimization.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/pushdown/redact.hpp"
#include "tenzir/pushdown/splunk.hpp"
#include "tenzir/pushdown/time_window.hpp"
#include "tenzir/startup_argument.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"
#include "tenzir/variant.hpp"

#include <boost/url/parse.hpp>
#include <folly/CancellationToken.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/WithCancellation.h>

#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_splunk {

namespace {

constexpr auto export_path
  = std::string_view{"/services/search/v2/jobs/export"};

constexpr auto spl2_dispatch_path
  = std::string_view{"/services/orchestrator/v1/spl2/modules/dispatch"};

struct FromSplunkArgs {
  located<std::string> url;
  // Exactly one of `search`, `spl2`, and `index` is set.
  Option<located<std::string>> search;
  Option<located<std::string>> spl2;
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
  Option<located<bool>> pushdown;
  location operator_location = location::unknown;
  // The stages that the operator took over, which run in Splunk after the
  // search (see `pushdown::spl_planner`), and the fields that downstream
  // needs from events that no stage reshapes.
  OptimizationArgs<opt::Projection, opt::Plan> optimization;
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
      // Splunk's SPL2 service answers errors with a message.
      if (auto it = envelope->find("message"); it != envelope->end()) {
        if (auto const* message = try_as<std::string>(it->second)) {
          return *message;
        }
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

auto make_url(std::string url, std::string_view path) -> std::string {
  if (url.ends_with('/')) {
    url.pop_back();
  }
  url += path;
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
  if (args.spl2) {
    return args.spl2->source;
  }
  TENZIR_ASSERT(args.index);
  return args.index->source;
}

/// What the operator searches.
auto spl_source(FromSplunkArgs const& args) -> pushdown::SplSource {
  if (args.search) {
    return {.search = args.search->inner, .indexes = {}};
  }
  if (args.spl2) {
    return {.search = args.spl2->inner, .indexes = {}, .spl2 = true};
  }
  TENZIR_ASSERT(args.index);
  auto names = index_names(args.index->inner);
  TENZIR_ASSERT(names);
  return {.search = None{}, .indexes = std::move(*names)};
}

/// Plans the search to send: the user's search or one for the indexes,
/// followed by the stages after the operator, which run in Splunk with
/// Splunk's semantics. Options may change how Splunk runs the search.
auto make_planner(FromSplunkArgs const& args)
  -> pushdown::Planner<pushdown::SplQuery> {
  auto const names = args.index ? index_names(args.index->inner) : None{};
  auto const sources
    = args.search.is_some() + args.spl2.is_some() + args.index.is_some();
  auto const valid
    = sources == 1 and (not args.index or (names and not names->empty()));
  return pushdown::spl_planner(
    valid ? spl_source(args) : pushdown::SplSource{},
    args.options ? Option<std::string>{"`options` may change how Splunk runs "
                                       "the search"}
    : not valid  ? Option<std::string>{"invalid arguments"}
                 : None{},
    args.optimization.projection);
}

/// Returns `bound` as an absolute time, if it is one: a time, or a string of
/// epoch seconds. Splunk resolves other strings, such as `-1h`, itself.
auto absolute_bound(data const& bound) -> Option<time> {
  if (auto const* x = try_as<time>(bound)) {
    return *x;
  }
  auto const* text = try_as<std::string>(bound);
  if (not text) {
    return None{};
  }
  auto digits = detail::trim(*text);
  auto value = double{};
  auto [end, error]
    = std::from_chars(digits.data(), digits.data() + digits.size(), value);
  if (digits.empty() or error != std::errc{}
      or end != digits.data() + digits.size() or not std::isfinite(value)
      or digits.find_first_of("eEnN") != std::string_view::npos) {
    return None{};
  }
  // Epoch seconds beyond what a `time` holds are no bound that the operator
  // can narrow, and converting them would overflow.
  constexpr auto limit = std::chrono::duration<double>{duration::max()};
  if (std::abs(value) >= limit.count()) {
    return None{};
  }
  return time{
    std::chrono::duration_cast<duration>(std::chrono::duration<double>{value})};
}

/// Narrows the time bound `bound` of the request to `seconds`, the bound of
/// `_time` that the filters of the search imply, if both are absolute.
auto narrow_bound(data const& bound, Option<int64_t> seconds, bool lower)
  -> data {
  auto current = absolute_bound(bound);
  // Rounding outwards can leave what a `time` holds at either end, where the
  // operator keeps the bound rather than overflow.
  auto narrowed = seconds.and_then([](int64_t x) {
    return checked_mul(x, int64_t{1'000'000'000});
  });
  if (not narrowed or not current
      or (lower ? time{duration{*narrowed}} <= *current
                : time{duration{*narrowed}} >= *current)) {
    return bound;
  }
  return fmt::to_string(*seconds);
}

/// Narrows the time bounds `earliest` and `latest` to the bounds `window` that
/// the filters of a search imply. Narrows only bounds that are absolute on
/// both sides, since a relative bound resolves on the server, and keeps both
/// bounds if the narrowed ones would cross, as when the filters admit no event:
/// Splunk rejects crossing bounds, while the filters in the search still hold.
auto narrow_bounds(data const& earliest, data const& latest,
                   pushdown::TimeWindow const& window)
  -> std::pair<data, data> {
  if (not absolute_bound(earliest) or not absolute_bound(latest)) {
    return {earliest, latest};
  }
  // Splunk includes `earliest_time` and excludes `latest_time`, so the
  // bounds round outwards to whole seconds.
  auto const [first, last] = pushdown::in_units(window, std::chrono::seconds{1},
                                                pushdown::Round::outward);
  auto lower = narrow_bound(earliest, first, true);
  auto upper = narrow_bound(latest, last.and_then([](int64_t x) {
    return checked_add(x, int64_t{1});
  }),
                            false);
  auto const begin = absolute_bound(lower);
  auto const end = absolute_bound(upper);
  TENZIR_ASSERT(begin and end);
  if (*begin >= *end) {
    return {earliest, latest};
  }
  return {std::move(lower), std::move(upper)};
}

/// The form body of a search request, within the time bounds `earliest` and
/// `latest`.
auto make_form_body(FromSplunkArgs const& args, std::string search,
                    data earliest, data latest) -> std::string {
  auto body = record{};
  body.emplace("search", std::move(search));
  body.emplace("earliest_time", std::move(earliest));
  body.emplace("latest_time", std::move(latest));
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

/// The JSON body that dispatches the SPL2 `search` as the statement `main` of
/// a module, within the time bounds `earliest` and `latest`, and with every
/// field extracted: by default, SPL2 extracts only indexed fields.
auto make_spl2_body(std::string_view search, data earliest, data latest)
  -> std::string {
  auto parameters = record{};
  parameters.emplace("earliest", std::move(earliest));
  parameters.emplace("latest", std::move(latest));
  parameters.emplace("extractFields", std::string{"all"});
  auto statements = record{};
  statements.emplace("main", std::move(parameters));
  auto body = record{};
  body.emplace("module", fmt::format("$main = {}", search));
  body.emplace("queryParameters", std::move(statements));
  auto json = to_json(body, {.style = no_style(), .oneline = true});
  TENZIR_ASSERT(json);
  return std::move(*json);
}

/// Logs the search that the operator sends, without its literals. A user's
/// search shows only as `<search>`, followed by the generated stages.
auto log_query(FromSplunkArgs const& args, pushdown::SplQuery const& query)
  -> void {
  auto const generated
    = pushdown::redact_query(query.generated, pushdown::spl_lexicon);
  if (args.search or args.spl2) {
    TENZIR_DEBUG("from_splunk sends <search>{}", generated);
  } else {
    TENZIR_DEBUG("from_splunk sends {}", generated);
  }
}

// Messages sent from the streaming fetch task to the operator.
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

struct EventResultBatch {
  std::vector<nova::Array<nova::Record>> records;
};

using EventStreamMessage = variant<EventResultBatch, FlushTick, StreamWarning,
                                   RetryWarning, SearchFailed, SearchRejected,
                                   RequestFailed, ParseFailed, StreamDone>;
using EventStreamQueue
  = folly::coro::BoundedQueue<EventStreamMessage, false, true>;

/// Forwards the `messages` of a search to `queue`. Returns whether one of them
/// ends the search.
auto forward_messages(std::vector<SplunkMessage> messages,
                      EventStreamQueue& queue) -> Task<bool> {
  for (auto& message : messages) {
    auto text = message.text.empty() ? std::string{"no details provided"}
                                     : std::move(message.text);
    if (detail::ascii_icase_equal(message.type, "ERROR")
        or detail::ascii_icase_equal(message.type, "FATAL")) {
      co_await queue.enqueue(SearchFailed{std::move(text)});
      co_return true;
    }
    auto type = message.type.empty() ? std::string{"message"} : message.type;
    if (detail::ascii_icase_equal(message.type, "WARN")) {
      co_await queue.enqueue(StreamWarning{std::move(type), std::move(text)});
      continue;
    }
    TENZIR_DEBUG("from_splunk: search returned {}: {}", type, text);
  }
  co_return false;
}

/// Returns the field `name` of `value`, if it is a record with one.
auto field_of(data const* value, std::string_view name) -> data const* {
  auto const* fields = value ? try_as<record>(*value) : nullptr;
  if (not fields) {
    return nullptr;
  }
  auto it = fields->find(name);
  return it == fields->end() ? nullptr : &it->second;
}

/// Dispatches the SPL2 module `body` through `pool`, whose URL has the path
/// `prefix`, and waits for its search job to finish, since Splunk exports the
/// results of a job only then. Returns the path that exports the results, or
/// `None` after enqueueing why there are none.
auto run_spl2_job(HttpPool& pool, std::string_view prefix, std::string body,
                  std::vector<http::Header> const& headers,
                  EventStreamQueue& queue) -> Task<Option<std::string>> {
  // The JSON of a successful response, which is `null` if it has none, or
  // `None` after enqueueing the error.
  auto parse
    = [&](Result<http::Response, std::string> result) -> Task<Option<data>> {
    if (result.is_err()) {
      co_await queue.enqueue(RequestFailed{std::move(result).unwrap_err()});
      co_return None{};
    }
    auto response = std::move(result).unwrap();
    if (not response.is_status_success()) {
      co_await queue.enqueue(SearchRejected{response_body_text(response.body)});
      co_return None{};
    }
    auto parsed = from_json(response.body);
    co_return parsed ? std::move(*parsed) : data{};
  };
  auto dispatched = co_await parse(co_await pool.post(
    fmt::format("{}{}", prefix, spl2_dispatch_path), std::move(body), headers));
  if (not dispatched) {
    co_return None{};
  }
  auto const* sid = field_of(
    field_of(field_of(&*dispatched, "queryParameters"), "main"), "sid");
  if (not sid or not is<std::string>(*sid)) {
    co_await queue.enqueue(
      ParseFailed{"expected a search ID in the Splunk response", None{}});
    co_return None{};
  }
  auto const job = fmt::format("{}/services/search/v2/jobs/{}", prefix,
                               curl::escape(as<std::string>(*sid)));
  auto delay = std::chrono::milliseconds{100};
  while (true) {
    auto status
      = co_await parse(co_await pool.get(job + "?output_mode=json", headers));
    if (not status) {
      co_return None{};
    }
    auto const* entries = field_of(&*status, "entry");
    auto const* entry
      = entries and is<list>(*entries) and not as<list>(*entries).empty()
          ? &as<list>(*entries).front()
          : nullptr;
    auto const* content = field_of(entry, "content");
    auto const* done = field_of(content, "isDone");
    if (not done or not is<bool>(*done)) {
      co_await queue.enqueue(ParseFailed{
        "expected the status of a search job in the Splunk response", None{}});
      co_return None{};
    }
    if (as<bool>(*done)) {
      if (co_await forward_messages(extract_messages(as<record>(*content)),
                                    queue)) {
        co_return None{};
      }
      co_return job + "/results/export?output_mode=json";
    }
    co_await sleep_for(delay);
    delay = std::min(2 * delay, std::chrono::milliseconds{1000});
  }
}

// Streams the Splunk export response and forwards results through the queue.
// Runs detached from the operator, so it must not touch the operator or its
// diagnostic handler.
auto run_export_request_events(
  std::string url, std::string target, std::string body,
  std::vector<http::Header> headers, HttpPoolConfig config,
  Arc<EventStreamQueue> queue,
  folly::Executor::KeepAlive<folly::IOExecutor> executor, bool raw, bool spl2)
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
    if (co_await forward_messages(extract_messages(envelope->data.get(0)),
                                  *queue)) {
      co_return true;
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
    // Erase the processed lines at once: erasing each one would move the rest
    // of the buffer per line.
    auto begin = size_t{0};
    auto pos = size_t{};
    while ((pos = buffer.find('\n', begin)) != std::string::npos) {
      auto const line = std::string_view{buffer}.substr(begin, pos - begin);
      if (co_await process_line(line)) {
        stopped = true;
        co_return true;
      }
      begin = pos + 1;
    }
    buffer.erase(0, begin);
    if (not records.empty()) {
      co_await queue->enqueue(EventResultBatch{std::move(records)});
      records.clear();
    }
    co_return false;
  };
  auto pool = HttpPool::make(std::move(executor), url, std::move(config));
  if (pool.is_err()) {
    co_await queue->enqueue(RequestFailed{std::move(pool).unwrap_err()});
    co_await queue->enqueue(StreamDone{});
    co_return;
  }
  auto client = std::move(pool).unwrap();
  // An SPL2 search runs as a job, whose results the operator exports after it
  // finished.
  auto results = Option<std::string>{};
  if (spl2) {
    TENZIR_ASSERT(target.ends_with(spl2_dispatch_path));
    auto prefix = std::string_view{target}.substr(
      0, target.size() - spl2_dispatch_path.size());
    results = co_await run_spl2_job(*client, prefix, std::move(body), headers,
                                    *queue);
    if (not results) {
      co_await queue->enqueue(StreamDone{});
      co_return;
    }
  }
  auto result
    = results
        ? co_await client->stream_get(std::move(*results), std::move(headers),
                                      std::move(callbacks))
        : co_await client->stream_post(std::move(target), std::move(body),
                                       std::move(headers),
                                       std::move(callbacks));
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
  explicit FromSplunkEvents(FromSplunkArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    events_read_
      = ctx.make_counter(MetricsLabel{"operator", "from_splunk"},
                         MetricsDirection::read, MetricsVisibility::external_,
                         MetricsUnit::events);
    query_ = pushdown::replan(make_planner(args_), args_.optimization.stages());
    auto headers = co_await resolve_headers(args_.headers, ctx);
    if (not headers) {
      done_ = true;
      co_return;
    }
    auto resolved_headers = std::move(*headers);
    auto url = make_url(args_.url.inner,
                        args_.spl2 ? spl2_dispatch_path : export_path);
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
      resolved_headers.emplace_back(
        "Content-Type",
        args_.spl2 ? "application/json" : "application/x-www-form-urlencoded");
    }
    if (not http::find(resolved_headers, "accept")) {
      resolved_headers.emplace_back("Accept", "application/json");
    }
    auto queue = Arc<EventStreamQueue>{std::in_place, 64};
    queue_ = queue;
    config->on_retry = [queue](std::string_view message) mutable {
      std::ignore = queue->try_enqueue(RetryWarning{std::string{message}});
    };
    url_ = url;
    log_query(args_, *query_);
    // The filters of a search for indexes may narrow its time bounds.
    auto [earliest, latest]
      = narrow_bounds(args_.earliest.inner, args_.latest.inner, query_->window);
    auto body = args_.spl2
                  ? make_spl2_body(query_->search, std::move(earliest),
                                   std::move(latest))
                  : make_form_body(args_, query_->search, std::move(earliest),
                                   std::move(latest));
    ctx.spawn_task([url = std::move(url), target = std::move(*target),
                    body = std::move(body),
                    headers = std::move(resolved_headers),
                    config = std::move(*config), queue = std::move(queue),
                    executor = ctx.io_executor(), raw = args_.raw.is_some(),
                    spl2 = args_.spl2.is_some(),
                    cancel = cancel_.getToken()]() mutable -> Task<void> {
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token, cancel);
      co_await folly::coro::co_withCancellation(
        token, run_export_request_events(std::move(url), std::move(target),
                                         std::move(body), std::move(headers),
                                         std::move(config), std::move(queue),
                                         std::move(executor), raw, spl2));
    });
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
        for (auto const& value : msg.records) {
          auto row_view = value.get(0);
          if (is_sentinel(row_view)) {
            sentinel_seen_ = true;
            continue;
          }
          if (not next_flush_) {
            next_flush_ = std::chrono::steady_clock::now()
                          + defaults::import::batch_timeout;
          }
          auto row = builder_.record();
          for (auto const& [name, field] : row_view) {
            append_field(row.field(name), name, field);
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
        diagnostic::warning("Splunk search returned {}: {}", msg.type, msg.text)
          .primary(search_source(args_))
          .note("search: {}", query_->search)
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
          .note("search: {}", query_->search)
          .emit(ctx);
        done_ = true;
        co_return;
      },
      [&](SearchRejected msg) -> Task<void> {
        diagnostic::error("Splunk rejected the search: {}", msg.text)
          .primary(search_source(args_))
          .note("search: {}", query_->search)
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
        co_await flush(push, ctx);
        if (query_->sentinel and not sentinel_seen_) {
          // Splunk answers a search that fails in `eval` or `stats` with an
          // empty response, which only the missing sentinel row reveals.
          pushdown::note_absorbed(diagnostic::error("Splunk search ended "
                                                    "without its last result")
                                    .primary(search_source(args_))
                                    .note("search: {}", query_->search),
                                  args_.optimization.stages(), "Splunk")
            .emit(ctx);
        }
        done_ = true;
        co_return;
      });
    // Any continuously ready message kind can starve the latency deadline.
    // Process the dequeued message first, but never flush after a fatal error.
    if (not done_ and next_flush_
        and std::chrono::steady_clock::now() >= *next_flush_) {
      co_await flush(push, ctx);
    }
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
    events
      = pushdown::apply_layout(std::move(events), query_->layout, ctx.dh());
    events_read_.add(events.active_count());
    co_await push(std::move(events));
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

  auto snapshot(Serde&) -> void override {
    // Restarting a streaming search would duplicate already committed results.
    diagnostic::error("from_splunk does not support checkpoints yet")
      .primary(args_.operator_location)
      .throw_();
  }

private:
  /// Whether `row` is the last row of a search with a sentinel, which has only
  /// the sentinel field.
  auto is_sentinel(nova::RowView<nova::Record> const& row) const -> bool {
    if (not query_->sentinel) {
      return false;
    }
    auto fields = size_t{0};
    auto found = false;
    for (auto const& [name, field] : row) {
      TENZIR_UNUSED(field);
      ++fields;
      found = found or name == *query_->sentinel;
    }
    return found and fields == 1;
  }

  /// Appends a field of a result row. A count or another number decodes as
  /// one also without type inference.
  auto append_field(nova::FieldBuilder builder, std::string_view name,
                    nova::RowView<nova::Data> const& field) const -> void {
    auto const* text = try_as<nova::RowView<nova::String>>(field);
    auto const decode = query_->layout.decode_of(name);
    if (text and decode == pushdown::Decode::integer) {
      if (auto count = pushdown::parse_spl_count(**text)) {
        builder.data(*count);
        return;
      }
    }
    if (text and decode == pushdown::Decode::number) {
      auto const value = std::string_view{**text};
      auto integer = int64_t{};
      auto real = double{};
      auto [end, error] = std::from_chars(value.begin(), value.end(), integer);
      if (error == std::errc{} and end == value.end()) {
        builder.data(integer);
        return;
      }
      if (parsers::real(value, real)) {
        builder.data(real);
        return;
      }
    }
    nova::append_row(builder, field);
  }

  FromSplunkArgs args_;
  Option<pushdown::SplQuery> query_;
  std::string url_;
  mutable Option<Arc<EventStreamQueue>> queue_;
  nova::ArrayBuilder<nova::Record> builder_;
  Option<std::chrono::steady_clock::time_point> next_flush_;
  /// Whether the sentinel row of the search arrived.
  bool sentinel_seen_ = false;
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
    auto d = Describer<FromSplunkArgs, FromSplunkEvents>{};
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
    pushdown::add_pushdown(d, &FromSplunkArgs::pushdown, make_planner);
    // Last, so that the other arguments keep their indices.
    auto spl2 = d.named("spl2", &FromSplunkArgs::spl2, "string");
    d.validate([=](DescribeCtx& ctx) -> Empty {
      // Fail fast so that one invalid argument yields one diagnostic.
      auto run = [&]() -> failure_or<void> {
        if (auto value = ctx.get(url)) {
          TRY(check_non_empty("url", *value, ctx));
        }
        auto sources = std::vector<std::pair<std::string_view, location>>{};
        for (auto [name, at] : {std::pair{"search", ctx.get_location(search)},
                                std::pair{"spl2", ctx.get_location(spl2)},
                                std::pair{"index", ctx.get_location(index)}}) {
          if (at) {
            sources.emplace_back(name, *at);
          }
        }
        if (sources.size() > 1) {
          diagnostic::error("`{}` and `{}` are mutually exclusive",
                            sources[0].first, sources[1].first)
            .primary(sources[0].second)
            .primary(sources[1].second)
            .emit(ctx);
          return failure::promise();
        }
        if (sources.empty()) {
          diagnostic::error("one of `index`, `search`, or `spl2` is required")
            .primary(ctx.operator_location())
            .hint("use `index` to read events, `search` to run SPL, or `spl2` "
                  "to run SPL2")
            .emit(ctx);
          return failure::promise();
        }
        for (auto [name, arg] :
             {std::pair{"search", search}, std::pair{"spl2", spl2}}) {
          if (auto value = ctx.get(arg)) {
            TRY(check_non_empty(name,
                                located{std::string{detail::trim(value->inner)},
                                        value->source},
                                ctx));
          }
        }
        if (auto at = ctx.get_location(options);
            at and sources[0].first == "spl2") {
          diagnostic::error("`options` does not apply to `spl2`")
            .primary(*at)
            .emit(ctx);
          return failure::promise();
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
    // The optimization inputs above take the projection, and the stages that
    // the operator takes over.
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_splunk::Plugin)
