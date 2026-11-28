//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/box.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/stringify.hpp>
#include <tenzir/nova_json_printer.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/try.hpp>
#include <tenzir/version.hpp>

#include <arrow/util/utf8.h>
#include <fmt/format.h>
#include <loki/push.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::loki {

namespace {

using namespace std::chrono_literals;
using namespace tenzir::si_literals;

constexpr auto default_request_timeout = 30s;
constexpr auto default_flush_interval = 1s;
constexpr auto default_max_batch_size = uint64_t{1'000};
constexpr auto default_max_batch_bytes = uint64_t{1_Mi};
constexpr auto default_parallel = uint64_t{8};
constexpr auto max_parallel = uint64_t{1'024};
constexpr auto max_timer_wait = 1h;
constexpr auto max_discard_response_bytes = size_t{64_Ki};

using Headers = std::vector<http::Header>;

struct ToLokiArgs {
  StartupArgument<nova::Secret> url;
  ast::expression labels = ast::root_field{
    ast::identifier{"labels", location::unknown},
    true,
  };
  ast::expression line = ast::root_field{
    ast::identifier{"line", location::unknown},
    true,
  };
  ast::expression timestamp = ast::root_field{
    ast::identifier{"timestamp", location::unknown},
    true,
  };
  ast::expression structured_metadata = ast::root_field{
    ast::identifier{"structured_metadata", location::unknown},
    true,
  };
  Option<ast::expression> tenant;
  located<std::string> encoding{"protobuf", location::unknown};
  bool compress = true;
  Option<StartupArgument<nova::Record>> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
  located<duration> flush_interval{default_flush_interval, location::unknown};
  located<uint64_t> max_batch_size{default_max_batch_size, location::unknown};
  located<uint64_t> max_batch_bytes{default_max_batch_bytes, location::unknown};
  located<uint64_t> parallel{default_parallel, location::unknown};
  location operator_location = location::unknown;
};

struct FlushTimeout {};
struct RequestDone {};

struct Completion {
  Result<http::Response, std::string> result;
  size_t bytes = 0;
  uint64_t events = 0;
};

/// Bounds concurrent HTTP requests globally to `capacity` while keeping at most
/// one request in flight per tenant key so retries cannot reorder batches for
/// the same tenant or stream.
class PerTenantWindow {
public:
  explicit PerTenantWindow(size_t capacity)
    : capacity_{std::clamp(capacity, size_t{1},
                           size_t{std::numeric_limits<uint32_t>::max()})},
      completions_{std::in_place, static_cast<uint32_t>(capacity_)} {
  }

  template <class F>
  auto poll(F&& handle) -> void {
    while (in_flight_ > 0) {
      auto item = completions_->try_dequeue();
      if (item.is_none()) {
        return;
      }
      finish(std::move(*item), handle);
    }
  }

  template <class F, class Stop>
  auto
  acquire_for(Option<std::string> const& tenant, F&& handle, Stop&& should_stop)
    -> Task<void> {
    poll(handle);
    while (
      not should_stop()
      and (in_flight_ >= capacity_ or in_flight_tenants_.contains(tenant))) {
      co_await take_one(handle);
    }
  }

  template <class F, class W>
  auto spawn(OpCtx& ctx, Option<std::string> tenant, F make_request, W wakeup)
    -> void {
    TENZIR_ASSERT(in_flight_ < capacity_);
    auto [_, inserted] = in_flight_tenants_.insert(tenant);
    TENZIR_ASSERT(inserted);
    ++in_flight_;
    std::ignore
      = ctx.spawn_task([completions = completions_, key = std::move(tenant),
                        make_request = std::move(make_request),
                        wakeup = std::move(wakeup)]() mutable -> Task<void> {
          auto value = co_await make_request();
          co_await completions->enqueue(Item{
            .key = std::move(key),
            .value = std::move(value),
          });
          wakeup();
        });
  }

  template <class F>
  auto drain(F&& handle) -> Task<void> {
    while (in_flight_ > 0) {
      co_await take_one(handle);
    }
  }

private:
  struct Item {
    Option<std::string> key;
    Completion value;
  };

  template <class F>
  auto finish(Item item, F& handle) -> void {
    auto const erased = in_flight_tenants_.erase(item.key);
    TENZIR_ASSERT(erased == 1);
    --in_flight_;
    handle(std::move(item.value));
  }

  template <class F>
  auto take_one(F& handle) -> Task<void> {
    TENZIR_ASSERT(in_flight_ > 0);
    auto item = co_await completions_->dequeue();
    finish(std::move(item), handle);
  }

  size_t capacity_;
  size_t in_flight_ = 0;
  std::set<Option<std::string>> in_flight_tenants_;
  Arc<BoundedQueue<Item, false, true>> completions_;
};

auto now_ns() -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::system_clock::now().time_since_epoch())
    .count();
}

auto to_timestamp_ns(nova::RowView<nova::Data> const& value)
  -> Option<int64_t> {
  return match(
    value, []<class T>(nova::RowView<T> const& x) -> Option<int64_t> {
      if constexpr (std::same_as<T, nova::Time>) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                 (*x).time_since_epoch())
          .count();
      } else if constexpr (std::same_as<T, nova::Int>) {
        return *x;
      } else if constexpr (std::same_as<T, nova::UInt>) {
        if (*x > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return None{};
        }
        return static_cast<int64_t>(*x);
      } else {
        return None{};
      }
    });
}

auto print_compact_json(nova::RowView<nova::Data> const& value,
                        nova::json_printer& printer) -> std::string {
  printer.print(value);
  auto const bytes = printer.bytes();
  return std::string{reinterpret_cast<char const*>(bytes.data()), bytes.size()};
}

auto to_line_string(nova::RowView<nova::Data> const& value,
                    nova::json_printer& printer, location loc,
                    diagnostic_handler& dh) -> Option<std::string> {
  return match(
    value,
    [&](nova::RowView<nova::Null>) -> Option<std::string> {
      diagnostic::warning("log line is `null`, skipping event")
        .primary(loc)
        .emit(dh);
      return None{};
    },
    [](nova::RowView<nova::String> str) -> Option<std::string> {
      return std::string{*str};
    },
    [&](nova::RowView<nova::Blob> blob) -> Option<std::string> {
      auto const bytes = *blob;
      if (not arrow::util::ValidateUTF8(
            reinterpret_cast<uint8_t const*>(bytes.data()), bytes.size())) {
        diagnostic::warning("expected `blob` to contain valid UTF-8 data")
          .primary(loc)
          .emit(dh);
        return None{};
      }
      return std::string{reinterpret_cast<char const*>(bytes.data()),
                         bytes.size()};
    },
    [&](auto const&) -> Option<std::string> {
      return print_compact_json(value, printer);
    });
}

auto to_metadata_string(nova::RowView<nova::Data> const& value,
                        nova::json_printer& printer, location loc,
                        diagnostic_handler& dh) -> Option<std::string> {
  if (is<nova::RowView<nova::Null>>(value)) {
    return None{};
  }
  if (auto const* str = try_as<nova::RowView<nova::String>>(value)) {
    return std::string{**str};
  }
  if (is<nova::RowView<nova::Record>>(value)
      or is<nova::RowView<nova::List>>(value)) {
    return print_compact_json(value, printer);
  }
  auto result = nova::stringify(value);
  if (not result) {
    diagnostic::warning("expected `blob` to contain valid UTF-8 data")
      .primary(loc)
      .emit(dh);
  }
  return result;
}

auto resolve_endpoint(OpCtx& ctx, ToLokiArgs const& args,
                      std::string& resolved_url, Headers& resolved_headers)
  -> Task<failure_or<void>> {
  CO_TRY(resolved_url, co_await args.url.evaluate_string(ctx, "url"));
  if (resolved_url.empty()) {
    diagnostic::error("`url` must not be empty").primary(args.url).emit(ctx);
    co_return failure::promise();
  }
  if (args.headers) {
    CO_TRY(auto headers, co_await args.headers->evaluate(ctx));
    auto const has_tenant = args.tenant.has_value();
    auto seen_org_id = false;
    auto invalid = false;
    // Omit source-line excerpts for all `headers=` runtime validation errors
    // so inline credentials in the same record literal are never echoed into
    // diagnostics.
    for (auto const& [name, _] : headers) {
      if (not is_valid_header_name(name)) {
        diagnostic::error("`to_loki` received an invalid HTTP header name")
          .note("HTTP header names must contain RFC 9110 token characters only")
          .emit(ctx);
        invalid = true;
        continue;
      }
      if (is_reserved_header(name, has_tenant)) {
        auto diag
          = diagnostic::error("`to_loki` header `{}` is reserved", name);
        if (detail::ascii_icase_equal(name, "x-scope-orgid")) {
          diag = std::move(diag).note(
            "`X-Scope-OrgID` is managed per batch when `tenant` is specified");
        } else {
          diag = std::move(diag).note("this header is managed by `to_loki`");
        }
        std::move(diag).emit(ctx);
        invalid = true;
        continue;
      }
      if (detail::ascii_icase_equal(name, "x-scope-orgid")) {
        if (seen_org_id) {
          diagnostic::error("`to_loki` received duplicate `X-Scope-OrgID` "
                            "headers")
            .emit(ctx);
          invalid = true;
        }
        seen_org_id = true;
      }
    }
    if (invalid) {
      co_return failure::promise();
    }
    resolved_headers.reserve(headers.size());
    for (auto const& [name, value] : headers) {
      auto resolved_value = std::string{};
      if (auto const* str = try_as<nova::String>(&value)) {
        resolved_value = *str;
      } else if (auto const* secret = try_as<nova::Secret>(&value)) {
        auto revealed
          = nova::reveal_string(*secret, name, location::unknown, ctx.dh());
        if (not revealed) {
          co_return failure::promise();
        }
        resolved_value = std::move(*revealed);
      } else {
        diagnostic::error(
          "`to_loki` header `{}` value must be `string` or `secret`", name)
          .emit(ctx);
        invalid = true;
        continue;
      }
      if (not is_valid_header_value(resolved_value)) {
        diagnostic::error("`to_loki` HTTP header `{}` contains invalid control "
                          "characters",
                          name)
          .emit(ctx);
        invalid = true;
        continue;
      }
      if (detail::ascii_icase_equal(name, "x-scope-orgid")
          and not is_valid_tenant_id(resolved_value)) {
        diagnostic::error(
          "`to_loki` has an invalid Loki tenant ID in `X-Scope-OrgID` header")
          .note(
            "tenant IDs must be 1..150 bytes of ASCII letters, digits, or "
            "`!`, `-`, `_`, `.`, `*`, `'`, `(`, `)`, and must not be `.` or "
            "`..`")
          .emit(ctx);
        invalid = true;
        continue;
      }
      resolved_headers.push_back({name, std::move(resolved_value)});
    }
    if (invalid) {
      co_return failure::promise();
    }
  }
  co_return {};
}

class ToLoki final : public Operator<nova::Events, void> {
public:
  explicit ToLoki(ToLokiArgs args)
    : args_{std::move(args)},
      buffer_{
        args_.max_batch_size.inner,
        args_.max_batch_bytes.inner,
        args_.flush_interval.inner,
        detail::saturating_mul(args_.max_batch_size.inner,
                               std::clamp<uint64_t>(args_.parallel.inner, 4,
                                                    32)),
        detail::saturating_mul(args_.max_batch_bytes.inner,
                               std::clamp<uint64_t>(args_.parallel.inner, 4,
                                                    32)),
      },
      window_{args_.parallel.inner},
      wakeup_queue_{
        std::in_place,
        detail::narrow_cast<uint32_t>(
          std::clamp<uint64_t>(args_.parallel.inner + 4, 16, 4'096)),
      } {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto prepare = [&](ast::expression const& expr,
                       Option<nova::Evaluator>& target) -> Task<bool> {
      auto evaluator = co_await nova::Evaluator::make(expr, ctx);
      if (not evaluator) {
        failed_ = true;
        co_return false;
      }
      target.emplace(std::move(*evaluator));
      co_return true;
    };
    if (not co_await prepare(args_.labels, labels_evaluator_)) {
      co_return;
    }
    if (not co_await prepare(args_.line, line_evaluator_)) {
      co_return;
    }
    if (not co_await prepare(args_.timestamp, timestamp_evaluator_)) {
      co_return;
    }
    if (not co_await prepare(args_.structured_metadata,
                             structured_metadata_evaluator_)) {
      co_return;
    }
    if (args_.tenant
        and not co_await prepare(*args_.tenant, tenant_evaluator_)) {
      co_return;
    }
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_loki"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_loki"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto parsed_encoding = parse_encoding(args_.encoding.inner);
    TENZIR_ASSERT(parsed_encoding);
    encoding_ = *parsed_encoding;
    auto url = std::string{};
    if (auto result = co_await resolve_endpoint(ctx, args_, url, headers_);
        result.is_error()) {
      failed_ = true;
      co_return;
    }
    auto config
      = http::make_http_pool_config(args_.tls, url, args_.url.get_location(),
                                    ctx.dh(), get_timeout(),
                                    ctx.actor_system().config());
    if (config.is_error()) {
      failed_ = true;
      co_return;
    }
    auto normalized = normalize_push_url(url);
    if (normalized.is_err()) {
      diagnostic::error("invalid Loki URL: {}",
                        std::move(normalized).unwrap_err())
        .primary(args_.url)
        .emit(ctx);
      failed_ = true;
      co_return;
    }
    auto push_url = std::move(normalized).unwrap();
    request_target_ = std::move(push_url.target);
    config->connection_timeout = get_connection_timeout();
    config->max_retry_count = get_max_retry_count();
    config->retry_delay = get_retry_delay();
    // Fan concurrent requests out across separate connections so `parallel > 1`
    // avoids HTTP/1.1 head-of-line serialization.
    config->max_concurrent_streams_per_connection = 1;
    config->on_retry
      = [dh = &ctx.dh(), loc = args_.operator_location](std::string_view msg) {
          diagnostic::warning("{}", msg).primary(loc).emit(*dh);
        };
    if (not http::find(headers_, "user-agent")) {
      http::set(headers_, "User-Agent",
                fmt::format("Tenzir/{}", version::version));
    }
    auto pool = HttpPool::make(ctx.io_executor(), std::move(push_url.url),
                               std::move(*config));
    if (pool.is_err()) {
      diagnostic::error("failed to initialize HTTP client: {}",
                        classify_transport_error(pool.unwrap_err()))
        .primary(args_.url)
        .emit(ctx);
      failed_ = true;
      co_return;
    }
    pool_ = std::move(pool).unwrap();
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    poll(ctx);
    if (failed_ or not pool_ or not input.mask.any()) {
      co_return;
    }
    auto eval_ctx = nova::EvalCtx{ctx.dh()};
    auto labels_col = labels_evaluator_->eval(input, eval_ctx);
    auto line_col = line_evaluator_->eval(input, eval_ctx);
    auto timestamp_col = timestamp_evaluator_->eval(input, eval_ctx);
    auto metadata_col = structured_metadata_evaluator_->eval(input, eval_ctx);
    auto tenant_col = Option<nova::Array<nova::Data>>{};
    if (tenant_evaluator_) {
      tenant_col = tenant_evaluator_->eval(input, eval_ctx);
    }
    auto const now = BatchBuffer::Clock::now();
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto labels = extract_labels(labels_col.get(row), ctx.dh());
      if (not labels) {
        continue;
      }
      auto line = to_line_string(line_col.get(row), json_printer_,
                                 expr_location(args_.line), ctx.dh());
      if (not line) {
        continue;
      }
      auto timestamp_ns = extract_timestamp(timestamp_col.get(row), ctx.dh());
      if (not timestamp_ns) {
        continue;
      }
      auto structured_metadata
        = extract_structured_metadata(metadata_col.get(row), ctx.dh());
      auto tenant = Option<std::string>{};
      if (tenant_col) {
        auto parsed_tenant = extract_tenant(tenant_col->get(row), ctx.dh());
        if (not parsed_tenant) {
          continue;
        }
        tenant = std::move(*parsed_tenant);
      }
      auto entry = Entry{
        .timestamp_ns = *timestamp_ns,
        .line = std::move(*line),
        .structured_metadata = std::move(structured_metadata),
      };
      auto ready = buffer_.add(std::move(tenant), std::move(*labels),
                               std::move(entry), now);
      if (not ready.empty()) {
        co_await send_batches(std::move(ready), ctx);
        if (failed_) {
          co_return;
        }
      }
    }
    arm_flush_timer(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await wakeup_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    // Handle completed requests before timers so failures surface promptly.
    poll(ctx);
    if (result.try_as<RequestDone>()) {
      co_return;
    }
    TENZIR_ASSERT(result.try_as<FlushTimeout>());
    timer_armed_ = false;
    if (failed_ or not pool_) {
      co_return;
    }
    co_await send_batches(buffer_.take_expired(BatchBuffer::Clock::now()), ctx);
    arm_flush_timer(ctx);
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    if (not failed_ and pool_) {
      co_await send_batches(buffer_.take_all(), ctx);
    }
    co_await drain(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (not failed_ and pool_) {
      co_await send_batches(buffer_.take_all(), ctx);
    }
    co_await drain(ctx);
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return failed_ or not pool_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto expr_location(ast::expression const& expr) const -> location {
    auto const loc = expr.get_location();
    return loc ? loc : args_.operator_location;
  }

  auto extract_labels(nova::RowView<nova::Data> const& value,
                      diagnostic_handler& dh) -> Option<Labels> {
    auto const loc = expr_location(args_.labels);
    auto const* rec = try_as<nova::RowView<nova::Record>>(value);
    if (not rec) {
      diagnostic::warning("`labels` must evaluate to a record, skipping event")
        .primary(loc)
        .emit(dh);
      return None{};
    }
    auto result = Labels{};
    for (auto [key, field_val] : *rec) {
      if (not is_valid_label_name(key)) {
        diagnostic::warning("invalid Loki label name, skipping event")
          .primary(loc)
          .note("Loki label names must match `[a-zA-Z_][a-zA-Z0-9_]*`")
          .emit(dh);
        return None{};
      }
      if (is<nova::RowView<nova::Null>>(field_val)) {
        continue;
      }
      auto str = to_metadata_string(field_val, json_printer_, loc, dh);
      if (not str) {
        return None{};
      }
      if (str->empty()) {
        continue;
      }
      result.emplace_back(std::string{key}, std::move(*str));
    }
    if (result.empty()) {
      diagnostic::warning(
        "Loki stream requires at least one non-empty label, skipping event")
        .primary(loc)
        .emit(dh);
      return None{};
    }
    std::ranges::sort(result, {}, [](auto const& pair) -> auto const& {
      return pair.first;
    });
    return result;
  }

  auto extract_timestamp(nova::RowView<nova::Data> const& value,
                         diagnostic_handler& dh) const -> Option<int64_t> {
    if (is<nova::RowView<nova::Null>>(value)) {
      return now_ns();
    }
    auto parsed = to_timestamp_ns(value);
    if (not parsed) {
      diagnostic::warning("invalid timestamp, skipping event")
        .primary(expr_location(args_.timestamp))
        .emit(dh);
      return None{};
    }
    return parsed;
  }

  auto extract_structured_metadata(nova::RowView<nova::Data> const& value,
                                   diagnostic_handler& dh) -> Labels {
    if (is<nova::RowView<nova::Null>>(value)) {
      return {};
    }
    auto const loc = expr_location(args_.structured_metadata);
    auto const* rec = try_as<nova::RowView<nova::Record>>(value);
    if (not rec) {
      diagnostic::warning(
        "`structured_metadata` must evaluate to a record, omitting structured "
        "metadata")
        .primary(loc)
        .emit(dh);
      return {};
    }
    auto result = Labels{};
    for (auto [key, field_val] : *rec) {
      if (key.empty()) {
        diagnostic::warning(
          "structured metadata key must not be empty, skipping field")
          .primary(loc)
          .emit(dh);
        continue;
      }
      if (is<nova::RowView<nova::Null>>(field_val)) {
        continue;
      }
      auto str = to_metadata_string(field_val, json_printer_, loc, dh);
      if (not str or str->empty()) {
        continue;
      }
      result.emplace_back(std::string{key}, std::move(*str));
    }
    std::ranges::sort(result, {}, [](auto const& pair) -> auto const& {
      return pair.first;
    });
    return result;
  }

  auto extract_tenant(nova::RowView<nova::Data> const& value,
                      diagnostic_handler& dh) -> Option<Option<std::string>> {
    TENZIR_ASSERT(args_.tenant);
    auto const loc = expr_location(*args_.tenant);
    if (is<nova::RowView<nova::Null>>(value)) {
      return Option<std::string>{None{}};
    }
    auto str = to_metadata_string(value, json_printer_, loc, dh);
    if (not str) {
      return None{};
    }
    if (not is_valid_tenant_id(*str)) {
      diagnostic::warning("invalid Loki tenant ID, skipping event")
        .primary(loc)
        .note("tenant IDs must be 1..150 bytes of ASCII letters, digits, or "
              "`!`, `-`, `_`, `.`, `*`, `'`, `(`, `)`, and must not be `.` or "
              "`..`")
        .emit(dh);
      return None{};
    }
    return Option<std::string>{std::move(*str)};
  }

  auto arm_flush_timer(OpCtx& ctx) -> void {
    if (timer_armed_) {
      return;
    }
    auto deadline = buffer_.next_deadline();
    if (not deadline) {
      return;
    }
    timer_armed_ = true;
    std::ignore = ctx.spawn_task(
      [queue = wakeup_queue_, deadline = *deadline]() mutable -> Task<void> {
        auto const now = BatchBuffer::Clock::now();
        // Clamp huge flush intervals so `sleep_until` never waits past a
        // bounded horizon; `process_task` re-arms if nothing expired yet.
        auto const wake_at
          = deadline > now and (deadline - now) > max_timer_wait
              ? now + max_timer_wait
              : deadline;
        co_await sleep_until(wake_at);
        co_await queue->enqueue(Any{FlushTimeout{}});
      });
  }

  auto send_batches(std::vector<TenantBatch> batches, OpCtx& ctx)
    -> Task<void> {
    for (auto& batch : batches) {
      if (failed_) {
        co_return;
      }
      co_await send_streams(batch.tenant, std::move(batch.streams), ctx);
    }
  }

  auto send_streams(Option<std::string> const& tenant,
                    std::vector<Stream> streams, OpCtx& ctx) -> Task<void> {
    if (failed_ or not pool_ or streams.empty()) {
      co_return;
    }
    sort_entries(streams);
    auto entry_count = uint64_t{0};
    for (auto const& stream : streams) {
      entry_count += stream.entries.size();
    }
    auto const is_singleton
      = streams.size() == 1 and streams.front().entries.size() == 1;
    auto uncompressed = std::string{};
    if (encoding_ == Encoding::protobuf) {
      auto serialized
        = serialize_protobuf(streams, args_.max_batch_bytes.inner);
      if (serialized.is_err()) {
        if (serialized.unwrap_err() == SerializeError::serialization_failed) {
          diagnostic::error("failed to serialize Loki protobuf push request")
            .primary(args_.operator_location)
            .emit(ctx);
          failed_ = true;
          co_return;
        }
        if (is_singleton) {
          diagnostic::error(
            "Loki push request with a single entry exceeds `max_batch_bytes`")
            .primary(args_.max_batch_bytes)
            .emit(ctx);
          failed_ = true;
          co_return;
        }
        auto second_half = split_streams(streams);
        co_await send_streams(tenant, std::move(streams), ctx);
        if (failed_) {
          co_return;
        }
        co_await send_streams(tenant, std::move(second_half), ctx);
        co_return;
      }
      uncompressed = std::move(serialized).unwrap();
    } else {
      uncompressed = serialize_json(streams);
      if (uncompressed.size() > args_.max_batch_bytes.inner) {
        if (is_singleton) {
          diagnostic::error(
            "Loki push request with a single entry exceeds `max_batch_bytes`")
            .primary(args_.max_batch_bytes)
            .emit(ctx);
          failed_ = true;
          co_return;
        }
        auto second_half = split_streams(streams);
        co_await send_streams(tenant, std::move(streams), ctx);
        if (failed_) {
          co_return;
        }
        co_await send_streams(tenant, std::move(second_half), ctx);
        co_return;
      }
    }
    // Count uncompressed wire bytes in operator throughput metrics.
    auto const payload_size = uncompressed.size();
    auto headers = headers_;
    if (tenant) {
      http::set(headers, "X-Scope-OrgID", *tenant);
    }
    auto body = std::string{};
    if (encoding_ == Encoding::protobuf) {
      auto compressed = snappy_compress(uncompressed);
      if (compressed.is_err()) {
        diagnostic::error("{}", std::move(compressed).unwrap_err())
          .primary(args_.operator_location)
          .emit(ctx);
        failed_ = true;
        co_return;
      }
      body = std::move(compressed).unwrap();
      http::set(headers, "Content-Type", "application/x-protobuf");
      // Loki's protobuf push endpoint expects raw Snappy blocks without a
      // Content-Encoding header (matching Promtail and Grafana Alloy).
      http::set(headers, "Content-Length", fmt::to_string(body.size()));
    } else {
      http::set(headers, "Content-Type", "application/json");
      auto encoded
        = args_.compress
            ? http::compress_request_body(std::move(uncompressed), "gzip",
                                          ctx.dh(), args_.operator_location)
            : http::EncodedRequestBody{.body = std::move(uncompressed)};
      http::add_request_body_headers(headers, encoded);
      body = std::move(encoded.body);
    }
    co_await window_.acquire_for(
      tenant,
      [&](Completion completion) {
        handle_completion(std::move(completion), ctx);
      },
      [this]() {
        return failed_;
      });
    if (failed_) {
      co_return;
    }
    window_.spawn(
      ctx, tenant,
      [pool = &**pool_, target = request_target_, body = std::move(body),
       headers = std::move(headers), payload_size,
       entry_count]() mutable -> Task<Completion> {
        // Record the first final non-retryable status received from Loki so
        // body-drain timeouts or errors never replay a definitive POST and the
        // received status remains authoritative.
        auto response_status = Option<uint16_t>{None{}};
        auto discarded = size_t{0};
        auto make_headers = [headers = std::move(headers), &response_status]()
          -> Task<Result<std::vector<http::Header>, std::string>> {
          if (response_status.has_value()) {
            co_return Err{std::string{"response status already received"}};
          }
          co_return headers;
        };
        auto callbacks = HttpStreamCallbacks{
          .on_headers =
            [&response_status, &discarded](http::Response const& resp) {
              if (resp.status_code >= 200) {
                response_status = resp.status_code;
                discarded = 0;
              }
            },
          .on_body
          = [&response_status, &discarded](std::string chunk) -> Task<bool> {
            // Cancel immediately on any body chunk of a non-204 response so
            // stalled or oversized error bodies never hang or buffer in memory.
            if (response_status
                and not is_push_success_status(*response_status)) {
              co_return true;
            }
            discarded += chunk.size();
            co_return discarded > max_discard_response_bytes;
          },
          // Leave `on_retry` empty and disable post-header retries so HttpPool
          // never replays once final non-retryable headers have arrived.
          .on_retry = {},
          .retry_after_headers = false,
        };
        auto result
          = co_await pool->stream_post(std::move(target), std::move(body),
                                       std::move(make_headers),
                                       std::move(callbacks));
        if (result.is_err() and response_status) {
          result = http::Response{
            .status_code = *response_status,
            .headers = {},
            .body = {},
          };
        }
        co_return Completion{
          .result = std::move(result),
          .bytes = payload_size,
          .events = entry_count,
        };
      },
      [queue = wakeup_queue_]() mutable {
        std::ignore = queue->try_enqueue(Any{RequestDone{}});
      });
  }

  auto poll(OpCtx& ctx) -> void {
    window_.poll([&](Completion completion) {
      handle_completion(std::move(completion), ctx);
    });
  }

  auto drain(OpCtx& ctx) -> Task<void> {
    co_await window_.drain([&](Completion completion) {
      handle_completion(std::move(completion), ctx);
    });
  }

  auto handle_completion(Completion completion, OpCtx& ctx) -> void {
    if (failed_) {
      return;
    }
    if (completion.result.is_err()) {
      diagnostic::error(
        "Loki push request failed: {}",
        classify_transport_error(completion.result.unwrap_err()))
        .primary(args_.operator_location)
        .emit(ctx);
      failed_ = true;
      return;
    }
    auto const status = completion.result.unwrap().status_code;
    if (is_push_success_status(status)) {
      bytes_write_counter_.add(completion.bytes);
      events_write_counter_.add(completion.events);
      return;
    }
    if (status == 200) {
      diagnostic::error("Loki push request returned unexpected HTTP status 200")
        .primary(args_.operator_location)
        .hint("Loki returns HTTP 204 No Content on push success; check that "
              "the URL targets a Loki push endpoint rather than a proxy or "
              "web page")
        .emit(ctx);
    } else if (status == 260) {
      diagnostic::error("Loki blocked log ingestion with HTTP status 260")
        .primary(args_.operator_location)
        .hint("ingestion is blocked on the Loki server or tenant; check Loki "
              "blocklist and quota settings")
        .emit(ctx);
    } else if (status == 400) {
      diagnostic::error("Loki rejected push request with HTTP status 400")
        .primary(args_.operator_location)
        .hint("check stream labels, timestamp range, and structured metadata "
              "limits")
        .emit(ctx);
    } else if (status == 401 or status == 403) {
      diagnostic::error("Loki authentication or authorization failed with HTTP "
                        "status {}",
                        status)
        .primary(args_.operator_location)
        .hint("check authentication headers and `tenant` / `X-Scope-OrgID` "
              "configuration")
        .emit(ctx);
    } else if (status == 404) {
      diagnostic::error("Loki push endpoint not found (HTTP status 404)")
        .primary(args_.operator_location)
        .hint("check the endpoint URL path (typically `/loki/api/v1/push`)")
        .emit(ctx);
    } else if (status == 413) {
      diagnostic::error(
        "Loki rejected oversized push payload with HTTP status 413")
        .primary(args_.operator_location)
        .hint("reduce `max_batch_bytes` or `max_batch_size`")
        .emit(ctx);
    } else if (status == 429) {
      diagnostic::error("Loki rate-limited push request with HTTP status 429")
        .primary(args_.operator_location)
        .hint("the retry budget is exhausted; reduce ingest rate or `parallel`")
        .emit(ctx);
    } else if (status >= 500 and status <= 599) {
      diagnostic::error("Loki server error with HTTP status {}", status)
        .primary(args_.operator_location)
        .hint("the retry budget is exhausted; check Loki service health")
        .emit(ctx);
    } else {
      diagnostic::error("Loki push request returned unexpected HTTP status {}",
                        status)
        .primary(args_.operator_location)
        .emit(ctx);
    }
    failed_ = true;
  }

  auto get_timeout() const -> std::chrono::milliseconds {
    if (args_.timeout) {
      return ceil_milliseconds(args_.timeout->inner);
    }
    return default_request_timeout;
  }

  auto get_connection_timeout() const -> std::chrono::milliseconds {
    if (args_.connection_timeout) {
      return ceil_milliseconds(args_.connection_timeout->inner);
    }
    return http::default_connection_timeout;
  }

  auto get_max_retry_count() const -> uint32_t {
    if (args_.max_retry_count) {
      return detail::narrow<uint32_t>(args_.max_retry_count->inner);
    }
    return http::default_max_retry_count;
  }

  auto get_retry_delay() const -> std::chrono::milliseconds {
    if (args_.retry_delay) {
      return ceil_milliseconds(args_.retry_delay->inner);
    }
    return http::default_retry_delay;
  }

  ToLokiArgs args_;
  Option<nova::Evaluator> labels_evaluator_;
  Option<nova::Evaluator> line_evaluator_;
  Option<nova::Evaluator> timestamp_evaluator_;
  Option<nova::Evaluator> structured_metadata_evaluator_;
  Option<nova::Evaluator> tenant_evaluator_;
  nova::json_printer json_printer_{{.style = no_style(), .oneline = true}};
  Encoding encoding_ = Encoding::protobuf;
  std::string request_target_;
  Headers headers_;
  Option<Box<HttpPool>> pool_;
  BatchBuffer buffer_;
  PerTenantWindow window_;
  mutable Arc<BoundedQueue<Any, false, true>> wakeup_queue_;
  bool timer_armed_ = false;
  bool failed_ = false;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
};

class ToLokiPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_loki";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToLokiArgs, ToLoki>{};
    d.positional("url", &ToLokiArgs::url);
    d.named_optional("labels", &ToLokiArgs::labels, "record");
    d.named_optional("line", &ToLokiArgs::line, "any");
    d.named_optional("timestamp", &ToLokiArgs::timestamp, "time");
    d.named_optional("structured_metadata", &ToLokiArgs::structured_metadata,
                     "record");
    d.named("tenant", &ToLokiArgs::tenant, "string");
    auto encoding = d.named_optional("encoding", &ToLokiArgs::encoding);
    auto compress = d.named("compress", &ToLokiArgs::compress);
    d.named("headers", &ToLokiArgs::headers, "record");
    auto tls_validator
      = tls_options{{.is_server = false}}.add_to_describer(d, &ToLokiArgs::tls);
    auto timeout = d.named("timeout", &ToLokiArgs::timeout);
    auto connection_timeout
      = d.named("connection_timeout", &ToLokiArgs::connection_timeout);
    auto max_retry_count
      = d.named("max_retry_count", &ToLokiArgs::max_retry_count);
    auto retry_delay = d.named("retry_delay", &ToLokiArgs::retry_delay);
    auto flush_interval
      = d.named_optional("flush_interval", &ToLokiArgs::flush_interval);
    auto max_batch_size
      = d.named_optional("max_batch_size", &ToLokiArgs::max_batch_size);
    auto max_batch_bytes
      = d.named_optional("max_batch_bytes", &ToLokiArgs::max_batch_bytes);
    auto parallel = d.named_optional("parallel", &ToLokiArgs::parallel);
    d.operator_location(&ToLokiArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      auto encoding_val = ctx.get(encoding).value_or(
        located<std::string>{"protobuf", location::unknown});
      auto parsed_encoding = parse_encoding(encoding_val.inner);
      if (not parsed_encoding) {
        diagnostic::error("unsupported Loki encoding `{}`", encoding_val.inner)
          .primary(encoding_val.source)
          .note("supported values are `protobuf` and `json`")
          .emit(ctx);
      } else if (*parsed_encoding == Encoding::protobuf
                 and ctx.get(compress) == Option<bool>{false}) {
        diagnostic::error(
          "`compress=false` is only supported with `encoding=\"json\"`")
          .primary(*ctx.get_location(compress))
          .note("Loki requires Snappy compression for protobuf push requests")
          .emit(ctx);
      }
      if (auto value = ctx.get(timeout);
          value and value->inner <= duration::zero()) {
        diagnostic::error("`timeout` must be a positive duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(connection_timeout);
          value and value->inner <= duration::zero()) {
        diagnostic::error("`connection_timeout` must be a positive duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_retry_count);
          value and value->inner > std::numeric_limits<uint32_t>::max()) {
        diagnostic::error("`max_retry_count` must be <= {}",
                          std::numeric_limits<uint32_t>::max())
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(retry_delay);
          value and value->inner < duration::zero()) {
        diagnostic::error("`retry_delay` must be a non-negative duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(flush_interval);
          value and value->inner <= duration::zero()) {
        diagnostic::error("`flush_interval` must be a positive duration")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_batch_size); value and value->inner == 0) {
        diagnostic::error("`max_batch_size` must be positive")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(max_batch_bytes); value and value->inner == 0) {
        diagnostic::error("`max_batch_bytes` must be positive")
          .primary(value->source)
          .emit(ctx);
      }
      if (auto value = ctx.get(parallel);
          value and (value->inner == 0 or value->inner > max_parallel)) {
        diagnostic::error("`parallel` must be between 1 and {}", max_parallel)
          .primary(value->source)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::loki

TENZIR_REGISTER_PLUGIN(tenzir::plugins::loki::ToLokiPlugin)
