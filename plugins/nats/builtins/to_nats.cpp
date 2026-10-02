//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"
#include "tenzir_plugins/nats/common.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova_json_printer.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/entity_path.hpp>
#include <tenzir/tql2/eval.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace tenzir::si_literals;
using namespace std::chrono_literals;

constexpr auto default_max_pending = uint64_t{1_Ki};
constexpr auto default_stall_wait = 200ms;

/// Builds the default `message=` expression used by `to_nats`.
auto default_message_expression() -> ast::expression {
  auto function
    = ast::entity{{ast::identifier{"print_ndjson", location::unknown}}};
  // Invariant: defaults bypass parser resolution in `OperatorPlugin`, so the
  // entity reference must be pre-resolved here.
  function.ref
    = entity_path{std::string{entity_pkg_std}, {"print_ndjson"}, entity_ns::fn};
  return ast::function_call{
    std::move(function),
    {ast::this_{location::unknown}},
    location::unknown,
    true,
  };
}

struct ToNatsArgs {
  located<std::string> subject;
  ast::expression message = default_message_expression();
  Option<ast::expression> headers;
  Option<located<secret>> url;
  Option<located<data>> tls;
  Option<located<data>> auth;
  uint64_t max_pending = default_max_pending;
  location op;
};

struct PublishAckError {
  uint64_t count = 0;
  std::string reason;
  int jetstream_error_code = 0;
};

auto make_publish_ack_error(jsPubAckErr const& error) -> PublishAckError {
  auto reason = fmt::format("{}: {}", nats_status_string(error.Err),
                            error.ErrText ? error.ErrText : "");
  return {
    .reason = std::move(reason),
    .jetstream_error_code = static_cast<int>(error.ErrCode),
  };
}

struct PublishAckFailures {
  auto record(jsPubAckErr const& error) -> void {
    auto guard = std::lock_guard{mutex};
    ++count;
    if (first_error.reason.empty()) {
      first_error = make_publish_ack_error(error);
    }
  }

  auto drain() -> PublishAckError {
    auto guard = std::lock_guard{mutex};
    auto result = std::exchange(first_error, {});
    result.count = std::exchange(count, uint64_t{0});
    return result;
  }

  std::mutex mutex;
  uint64_t count = 0;
  PublishAckError first_error;
};

void publish_error_handler(jsCtx*, jsPubAckErr* error, void* closure) {
  auto* failures = static_cast<PublishAckFailures*>(closure);
  TENZIR_ASSERT(failures);
  if (error) {
    failures->record(*error);
  }
}

namespace legacy {

auto add_header_values(natsMsg* msg, std::string const& key, list const& values,
                       ast::expression const& expr, diagnostic_handler& dh)
  -> bool {
  auto first = true;
  for (auto const& item : values) {
    if (is<caf::none_t>(item)) {
      continue;
    }
    auto const* value = try_as<std::string>(&item);
    if (not value) {
      diagnostic::warning("NATS header `{}` must contain only strings", key)
        .primary(expr)
        .note("event is skipped")
        .emit(dh);
      return false;
    }
    auto status = first ? natsMsgHeader_Set(msg, key.c_str(), value->c_str())
                        : natsMsgHeader_Add(msg, key.c_str(), value->c_str());
    first = false;
    if (status != NATS_OK) {
      emit_nats_error(
        diagnostic::warning("failed to set NATS header `{}`", key).primary(expr),
        status, dh);
      return false;
    }
  }
  return true;
}

auto add_headers(natsMsg* msg, data const& value, ast::expression const& expr,
                 diagnostic_handler& dh) -> bool {
  if (is<caf::none_t>(value)) {
    return true;
  }
  auto const* headers = try_as<record>(&value);
  if (not headers) {
    diagnostic::warning("`headers` must be a record, got `{}`",
                        type::infer(value).value_or(type{}).kind())
      .primary(expr)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  for (auto const& [key, header] : *headers) {
    if (is<caf::none_t>(header)) {
      continue;
    }
    if (auto const* str = try_as<std::string>(&header)) {
      auto status = natsMsgHeader_Set(msg, key.c_str(), str->c_str());
      if (status != NATS_OK) {
        emit_nats_error(diagnostic::warning("failed to set NATS header `{}`",
                                            key)
                          .primary(expr),
                        status, dh);
        return false;
      }
      continue;
    }
    if (auto const* list = try_as<tenzir::list>(&header)) {
      if (not add_header_values(msg, key, *list, expr, dh)) {
        return false;
      }
      continue;
    }
    diagnostic::warning("NATS header `{}` must be string or list<string>", key)
      .primary(expr)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  return true;
}

class ToNats final : public Operator<table_slice, void> {
public:
  explicit ToNats(ToNatsArgs args)
    : args_{std::move(args)}, ack_failures_{std::in_place} {
  }

  ToNats(ToNats const&) = delete;
  auto operator=(ToNats const&) -> ToNats& = delete;
  ToNats(ToNats&&) noexcept = default;
  auto operator=(ToNats&&) noexcept -> ToNats& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    write_bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_nats"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    write_events_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_nats"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto resolved
      = co_await resolve_connection_config(ctx, args_.url, args_.auth);
    if (not resolved) {
      done_ = true;
      co_return;
    }
    io_executor_ = ctx.io_executor();
    auto* evb = io_executor_->getEventBase();
    TENZIR_ASSERT(evb);
    auto options
      = make_nats_options(*resolved, args_.tls,
                          args_.url ? args_.url->source : location::unknown,
                          ctx.dh(), *evb, ctx.actor_system().config());
    if (not options) {
      done_ = true;
      co_return;
    }
    options_ = std::move(*options);
    auto* raw_connection = static_cast<natsConnection*>(nullptr);
    auto status = co_await spawn_blocking([&] {
      return natsConnection_Connect(&raw_connection, options_.get());
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to connect to NATS server")
                        .primary(args_.url ? args_.url->source
                                           : location::unknown),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    connection_ = nats_connection_ptr{raw_connection};
    auto js_options = jsOptions{};
    jsOptions_Init(&js_options);
    js_options.PublishAsync.MaxPending
      = detail::narrow_cast<int64_t>(args_.max_pending);
    js_options.PublishAsync.StallWait = default_stall_wait.count();
    js_options.PublishAsync.ErrHandler = publish_error_handler;
    js_options.PublishAsync.ErrHandlerClosure = &*ack_failures_;
    auto* raw_js = static_cast<jsCtx*>(nullptr);
    status = natsConnection_JetStream(&raw_js, connection_.get(), &js_options);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create JetStream context")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    js_ = js_ctx_ptr{raw_js};
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_ or input.rows() == 0 or not js_) {
      co_return;
    }
    publish_slice(std::move(input), ctx);
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    co_await complete_publishes(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await complete_publishes(ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto publish_slice(table_slice input, OpCtx& ctx) -> bool {
    if (done_ or input.rows() == 0 or not js_) {
      return true;
    }
    auto& dh = ctx.dh();
    auto messages = eval(args_.message, input, dh);
    auto headers = Option<multi_series>{};
    if (args_.headers) {
      headers = eval(*args_.headers, input, dh);
    }
    auto first_row = int64_t{0};
    auto written_bytes = uint64_t{0};
    for (auto const& message_series : messages) {
      auto ok = headers
                  ? publish_messages_with_headers(message_series, first_row,
                                                  *headers, ctx, written_bytes)
                  : publish_messages(message_series, ctx, written_bytes);
      if (not ok) {
        write_bytes_counter_.add(written_bytes);
        return false;
      }
      first_row += message_series.length();
    }
    write_bytes_counter_.add(written_bytes);
    drain_ack_errors(ctx);
    return true;
  }

  auto publish_payload(std::span<const std::byte> bytes, OpCtx& ctx,
                       uint64_t& written_bytes) -> bool {
    auto status
      = js_PublishAsync(js_.get(), args_.subject.inner.c_str(), bytes.data(),
                        detail::narrow_cast<int>(bytes.size()), nullptr);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to publish NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      return false;
    }
    written_bytes += bytes.size();
    write_events_counter_.add(1);
    return true;
  }

  auto publish_payload_with_headers(std::span<const std::byte> bytes,
                                    int64_t row, multi_series const& headers,
                                    OpCtx& ctx, uint64_t& written_bytes)
    -> bool {
    auto* raw_msg = static_cast<natsMsg*>(nullptr);
    auto status = natsMsg_Create(&raw_msg, args_.subject.inner.c_str(), nullptr,
                                 reinterpret_cast<char const*>(bytes.data()),
                                 detail::narrow_cast<int>(bytes.size()));
    auto msg = nats_msg_ptr{raw_msg};
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      return false;
    }
    if (not headers.is_null(row)) {
      auto header_data = materialize(headers.view3_at(row));
      if (not add_headers(msg.get(), header_data, *args_.headers, ctx.dh())) {
        return true;
      }
    }
    raw_msg = msg.release();
    status = js_PublishMsgAsync(js_.get(), &raw_msg, nullptr);
    msg.reset(raw_msg);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to publish NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      return false;
    }
    written_bytes += bytes.size();
    write_events_counter_.add(1);
    return true;
  }

  auto publish_messages(series const& messages, OpCtx& ctx,
                        uint64_t& written_bytes) -> bool {
    auto impl
      = [&](concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto const&
              array) {
          for (auto row = int64_t{0}; row < array.length(); ++row) {
            if (array.IsNull(row)) {
              diagnostic::warning("expected `string` or `blob`, got `null`")
                .primary(args_.message)
                .emit(ctx);
              continue;
            }
            if (not publish_payload(as_bytes(array.Value(row)), ctx,
                                    written_bytes)) {
              return false;
            }
          }
          return true;
        };
    return match(
      *messages.array,
      [&](concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto const&
            array) {
        return impl(array);
      },
      [&](auto const&) {
        diagnostic::warning("expected `string` or `blob`, got `{}`",
                            messages.type.kind())
          .primary(args_.message)
          .note("event is skipped")
          .emit(ctx);
        return true;
      });
  }

  auto publish_messages_with_headers(series const& messages, int64_t first_row,
                                     multi_series const& headers, OpCtx& ctx,
                                     uint64_t& written_bytes) -> bool {
    auto impl
      = [&](concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto const&
              array) {
          for (auto row = int64_t{0}; row < array.length(); ++row) {
            if (array.IsNull(row)) {
              diagnostic::warning("expected `string` or `blob`, got `null`")
                .primary(args_.message)
                .emit(ctx);
              continue;
            }
            if (not publish_payload_with_headers(as_bytes(array.Value(row)),
                                                 first_row + row, headers, ctx,
                                                 written_bytes)) {
              return false;
            }
          }
          return true;
        };
    return match(
      *messages.array,
      [&](concepts::one_of<arrow::BinaryArray, arrow::StringArray> auto const&
            array) {
        return impl(array);
      },
      [&](auto const&) {
        diagnostic::warning("expected `string` or `blob`, got `{}`",
                            messages.type.kind())
          .primary(args_.message)
          .note("event is skipped")
          .emit(ctx);
        return true;
      });
  }

  auto complete_publishes(OpCtx& ctx) -> Task<void> {
    if (not js_) {
      co_return;
    }
    auto status = co_await spawn_blocking([this] {
      return js_PublishAsyncComplete(js_.get(), nullptr);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to flush NATS publishes")
                        .primary(args_.subject.source),
                      status, ctx.dh());
    }
    drain_ack_errors(ctx);
  }

  auto drain_ack_errors(OpCtx& ctx) -> void {
    auto failures = ack_failures_->drain();
    if (failures.count != 0) {
      auto diag
        = diagnostic::error("{} NATS publish acknowledgment{} failed",
                            failures.count, failures.count == 1 ? "" : "s")
            .primary(args_.subject.source)
            .note("first error: {}", failures.reason);
      if (failures.jetstream_error_code != 0) {
        diag = std::move(diag).note("JetStream error code: {}",
                                    failures.jetstream_error_code);
      }
      std::move(diag).emit(ctx);
    }
  }

  ToNatsArgs args_;
  mutable Arc<PublishAckFailures> ack_failures_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  nats_options_ptr options_;
  nats_connection_ptr connection_;
  js_ctx_ptr js_;
  MetricsCounter write_bytes_counter_;
  MetricsCounter write_events_counter_;
  bool done_ = false;
};

} // namespace legacy

/// Returns the printer options if `expr` is a `print_json` or `print_ndjson`
/// call on `this` with constant `true` flags, such as the default `message`.
/// We print such messages directly from the events instead of evaluating the
/// expression.
auto try_make_json_printer(ast::expression const& expr)
  -> Option<json_printer_options> {
  auto const* call = try_as<ast::function_call>(expr);
  if (not call or not call->fn.ref.resolved()
      or call->fn.ref.pkg() != entity_pkg_std) {
    return None{};
  }
  auto const segments = call->fn.ref.segments();
  if (segments.size() != 1) {
    return None{};
  }
  auto compact = false;
  if (segments.front() == "print_ndjson") {
    compact = true;
  } else if (segments.front() != "print_json") {
    return None{};
  }
  if (call->args.empty() or not is<ast::this_>(call->args.front())) {
    return None{};
  }
  auto options = json_printer_options{
    .style = no_style(),
    .oneline = compact,
  };
  for (auto const& arg : std::span{call->args}.subspan(1)) {
    auto const* assignment = try_as<ast::assignment>(arg);
    if (not assignment) {
      return None{};
    }
    auto path = ast::field_path::try_from(assignment->left);
    if (not path) {
      return None{};
    }
    auto const option = path->path();
    if (option.size() != 1) {
      return None{};
    }
    auto const* constant = try_as<ast::constant>(assignment->right);
    if (not constant) {
      return None{};
    }
    auto const* flag = try_as<bool>(constant->value);
    if (not flag or not *flag) {
      return None{};
    }
    auto const& name = option.front().id.name;
    if (name == "strip") {
      options.omit_null_fields = true;
      options.omit_nulls_in_lists = true;
      options.omit_empty_records = true;
      options.omit_empty_lists = true;
    } else if (name == "strip_null_fields") {
      options.omit_null_fields = true;
    } else if (name == "strip_nulls_in_lists") {
      options.omit_nulls_in_lists = true;
    } else if (name == "strip_empty_records") {
      options.omit_empty_records = true;
    } else if (name == "strip_empty_lists") {
      options.omit_empty_lists = true;
    } else {
      return None{};
    }
  }
  return options;
}

auto type_name(nova::RowView<nova::Data> const& value) -> std::string_view {
  return match(value, []<class T>(nova::RowView<T> const&) {
    return nova::Type<T>::static_name;
  });
}

auto set_header(natsMsg* msg, std::string const& key, std::string_view value,
                bool append, ast::expression const& expr,
                diagnostic_handler& dh) -> bool {
  auto const str = std::string{value};
  auto status = append ? natsMsgHeader_Add(msg, key.c_str(), str.c_str())
                       : natsMsgHeader_Set(msg, key.c_str(), str.c_str());
  if (status != NATS_OK) {
    emit_nats_error(
      diagnostic::warning("failed to set NATS header `{}`", key).primary(expr),
      status, dh);
    return false;
  }
  return true;
}

auto add_header_values(natsMsg* msg, std::string const& key,
                       nova::RowView<nova::List> const& values,
                       ast::expression const& expr, diagnostic_handler& dh)
  -> bool {
  auto first = true;
  for (auto item : values) {
    if (is<nova::RowView<nova::Null>>(item)) {
      continue;
    }
    auto const* value = try_as<nova::RowView<nova::String>>(item);
    if (not value) {
      diagnostic::warning("NATS header `{}` must contain only strings", key)
        .primary(expr)
        .note("event is skipped")
        .emit(dh);
      return false;
    }
    if (not set_header(msg, key, **value, not first, expr, dh)) {
      return false;
    }
    first = false;
  }
  return true;
}

auto add_headers(natsMsg* msg, nova::RowView<nova::Data> const& value,
                 ast::expression const& expr, diagnostic_handler& dh) -> bool {
  if (is<nova::RowView<nova::Null>>(value)) {
    return true;
  }
  auto const* headers = try_as<nova::RowView<nova::Record>>(value);
  if (not headers) {
    diagnostic::warning("`headers` must be a record, got `{}`",
                        type_name(value))
      .primary(expr)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  for (auto const& [name, header] : *headers) {
    if (is<nova::RowView<nova::Null>>(header)) {
      continue;
    }
    auto const key = std::string{name};
    if (auto const* str = try_as<nova::RowView<nova::String>>(header)) {
      if (not set_header(msg, key, **str, false, expr, dh)) {
        return false;
      }
      continue;
    }
    if (auto const* list = try_as<nova::RowView<nova::List>>(header)) {
      if (not add_header_values(msg, key, *list, expr, dh)) {
        return false;
      }
      continue;
    }
    diagnostic::warning("NATS header `{}` must be string or list<string>", key)
      .primary(expr)
      .note("event is skipped")
      .emit(dh);
    return false;
  }
  return true;
}

/// Bounds the wait for each publish acknowledgment, so that a missing one
/// fails the publish instead of holding its permit, and `finalize()`, forever.
constexpr auto publish_ack_timeout = std::chrono::milliseconds{30s};

/// The NATS resources of one `to_nats` instance. The JetStream context invokes
/// `publish_ack_handler` with a pointer to its publisher, so the publisher must
/// not move.
class Publisher {
public:
  explicit Publisher(uint64_t max_pending)
    : permits{detail::narrow_cast<size_t>(max_pending)} {
  }

  Publisher(Publisher const&) = delete;
  auto operator=(Publisher const&) -> Publisher& = delete;
  Publisher(Publisher&&) = delete;
  auto operator=(Publisher&&) -> Publisher& = delete;

  folly::Executor::KeepAlive<folly::IOExecutor> io_executor;
  /// One permit per publish that may be in flight. We enforce `_max_pending`
  /// ourselves, because nats.c blocks the publishing thread for up to its
  /// stall wait once the limit is reached, and then fails the publish.
  Semaphore permits;
  PublishAckFailures failures;
  // Declared in dependency order, so that destruction releases the JetStream
  // context before the connection, and the connection before the event base
  // that it runs on.
  nats_options_ptr options;
  nats_connection_ptr connection;
  js_ctx_ptr js;
};

/// Settles one publish. nats.c invokes this exactly once per published
/// message: with the acknowledgment, the error from the server, or a timeout.
void publish_ack_handler(jsCtx*, natsMsg* msg, jsPubAck*, jsPubAckErr* error,
                         void* closure) {
  auto* publisher = static_cast<Publisher*>(closure);
  TENZIR_ASSERT(publisher);
  if (error) {
    publisher->failures.record(*error);
  }
  natsMsg_Destroy(msg);
  publisher->permits.add_permit();
}

class ToNats final : public Operator<nova::Events, void> {
public:
  explicit ToNats(ToNatsArgs args)
    : args_{std::move(args)}, publisher_{std::in_place, args_.max_pending} {
  }

  ToNats(ToNats const&) = delete;
  auto operator=(ToNats const&) -> ToNats& = delete;
  ToNats(ToNats&&) noexcept = default;
  auto operator=(ToNats&&) noexcept -> ToNats& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    if (auto options = try_make_json_printer(args_.message)) {
      printer_.emplace(std::move(*options));
    } else {
      auto evaluator = co_await nova::Evaluator::make(args_.message, ctx);
      if (not evaluator) {
        done_ = true;
        co_return;
      }
      message_.emplace(std::move(*evaluator));
    }
    if (args_.headers) {
      auto evaluator = co_await nova::Evaluator::make(*args_.headers, ctx);
      if (not evaluator) {
        done_ = true;
        co_return;
      }
      headers_.emplace(std::move(*evaluator));
    }
    write_bytes_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_nats"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    write_events_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_nats"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    auto resolved
      = co_await resolve_connection_config(ctx, args_.url, args_.auth);
    if (not resolved) {
      done_ = true;
      co_return;
    }
    publisher_->io_executor = ctx.io_executor();
    auto* evb = publisher_->io_executor->getEventBase();
    TENZIR_ASSERT(evb);
    auto options
      = make_nats_options(*resolved, args_.tls,
                          args_.url ? args_.url->source : location::unknown,
                          ctx.dh(), *evb, ctx.actor_system().config());
    if (not options) {
      done_ = true;
      co_return;
    }
    publisher_->options = std::move(*options);
    auto* raw_connection = static_cast<natsConnection*>(nullptr);
    auto status = co_await spawn_blocking([&] {
      return natsConnection_Connect(&raw_connection, publisher_->options.get());
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to connect to NATS server")
                        .primary(args_.url ? args_.url->source
                                           : location::unknown),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    publisher_->connection = nats_connection_ptr{raw_connection};
    auto js_options = jsOptions{};
    jsOptions_Init(&js_options);
    // Disable the limit of nats.c in favor of the permits.
    js_options.PublishAsync.MaxPending = 0;
    js_options.PublishAsync.AckHandler = publish_ack_handler;
    js_options.PublishAsync.AckHandlerClosure = &*publisher_;
    auto* raw_js = static_cast<jsCtx*>(nullptr);
    status = natsConnection_JetStream(&raw_js, publisher_->connection.get(),
                                      &js_options);
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create JetStream context")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      co_return;
    }
    publisher_->js = js_ctx_ptr{raw_js};
    jsPubOptions_Init(&publish_options_);
    publish_options_.MaxWait = publish_ack_timeout.count();
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    if (done_ or not input.mask.any() or not publisher_->js) {
      co_return;
    }
    co_await publish_events(input, ctx);
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    co_await complete_publishes(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    co_await complete_publishes(ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto publish_events(nova::Events const& input, OpCtx& ctx) -> Task<void> {
    auto& dh = ctx.dh();
    auto messages = Option<nova::Array<nova::Data>>{};
    if (message_) {
      messages = message_->eval(input, nova::EvalCtx{dh});
    }
    auto headers = Option<nova::Array<nova::Data>>{};
    if (headers_) {
      headers = headers_->eval(input, nova::EvalCtx{dh});
    }
    auto written_bytes = uint64_t{0};
    // Warn once per type and batch to not flood the output.
    auto warned_types = std::vector<std::string_view>{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto payload = std::span<std::byte const>{};
      if (printer_) {
        printer_->print(input.data.get(row));
        payload = printer_->bytes();
      } else {
        TENZIR_ASSERT(messages);
        auto message = messages->get(row);
        if (auto const* text = try_as<nova::RowView<nova::String>>(message)) {
          payload = as_bytes(**text);
        } else if (auto const* bytes
                   = try_as<nova::RowView<nova::Blob>>(message)) {
          payload = as_bytes(**bytes);
        } else {
          auto const name = type_name(message);
          if (std::ranges::find(warned_types, name) == warned_types.end()) {
            warned_types.push_back(name);
            diagnostic::warning("expected `string` or `blob`, got `{}`", name)
              .primary(args_.message)
              .note("event is skipped")
              .emit(dh);
          }
          continue;
        }
      }
      auto msg = make_message(payload, ctx);
      if (not msg) {
        break;
      }
      if (headers
          and not add_headers(msg->get(), headers->get(row), *args_.headers,
                              dh)) {
        continue;
      }
      if (not co_await publish(std::move(*msg), ctx)) {
        break;
      }
      written_bytes += payload.size();
    }
    write_bytes_counter_.add(written_bytes);
    drain_ack_errors(ctx);
  }

  auto make_message(std::span<std::byte const> payload, OpCtx& ctx)
    -> Option<nats_msg_ptr> {
    auto* raw_msg = static_cast<natsMsg*>(nullptr);
    auto status = natsMsg_Create(&raw_msg, args_.subject.inner.c_str(), nullptr,
                                 reinterpret_cast<char const*>(payload.data()),
                                 detail::narrow_cast<int>(payload.size()));
    auto msg = nats_msg_ptr{raw_msg};
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to create NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      return None{};
    }
    return msg;
  }

  /// Publishes `msg` once fewer than `_max_pending` publishes are in flight.
  auto publish(nats_msg_ptr msg, OpCtx& ctx) -> Task<bool> {
    auto& permits = publisher_->permits;
    if (not permits.try_consume()) {
      co_await permits.consume();
    }
    auto* raw_msg = msg.release();
    auto status
      = js_PublishMsgAsync(publisher_->js.get(), &raw_msg, &publish_options_);
    // nats.c clears the pointer if it took ownership.
    msg.reset(raw_msg);
    if (status != NATS_OK) {
      permits.add_permit();
      emit_nats_error(diagnostic::error("failed to publish NATS message")
                        .primary(args_.subject.source),
                      status, ctx.dh());
      done_ = true;
      co_return false;
    }
    write_events_counter_.add(1);
    co_return true;
  }

  auto complete_publishes(OpCtx& ctx) -> Task<void> {
    if (not publisher_->js) {
      co_return;
    }
    auto status = co_await spawn_blocking([this] {
      return js_PublishAsyncComplete(publisher_->js.get(), nullptr);
    });
    if (status != NATS_OK) {
      emit_nats_error(diagnostic::error("failed to flush NATS publishes")
                        .primary(args_.subject.source),
                      status, ctx.dh());
    }
    drain_ack_errors(ctx);
  }

  auto drain_ack_errors(OpCtx& ctx) -> void {
    auto failures = publisher_->failures.drain();
    if (failures.count != 0) {
      auto diag
        = diagnostic::error("{} NATS publish acknowledgment{} failed",
                            failures.count, failures.count == 1 ? "" : "s")
            .primary(args_.subject.source)
            .note("first error: {}", failures.reason);
      if (failures.jetstream_error_code != 0) {
        diag = std::move(diag).note("JetStream error code: {}",
                                    failures.jetstream_error_code);
      }
      std::move(diag).emit(ctx);
    }
  }

  ToNatsArgs args_;
  Arc<Publisher> publisher_;
  jsPubOptions publish_options_ = {};
  Option<nova::json_printer> printer_;
  Option<nova::Evaluator> message_;
  Option<nova::Evaluator> headers_;
  MetricsCounter write_bytes_counter_;
  MetricsCounter write_events_counter_;
  bool done_ = false;
};

class ToNatsPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_nats";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToNatsArgs, legacy::ToNats, ToNats>{};
    auto subject_arg = d.positional("subject", &ToNatsArgs::subject);
    d.named_optional("message", &ToNatsArgs::message, "blob|string");
    d.named("headers", &ToNatsArgs::headers, "record");
    auto url_arg = d.named("url", &ToNatsArgs::url);
    auto tls_arg = d.named("tls", &ToNatsArgs::tls, "record");
    auto auth_arg = d.named("auth", &ToNatsArgs::auth, "record");
    auto max_pending_arg
      = d.named_optional("_max_pending", &ToNatsArgs::max_pending);
    d.operator_location(&ToNatsArgs::op);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto subject, ctx.get(subject_arg));
      if (subject.inner.empty()) {
        diagnostic::error("`subject` must not be empty")
          .primary(subject.source)
          .emit(ctx);
      }
      if (auto url = ctx.get(url_arg);
          url and not url->inner.is_all_literal()) {
        // Managed secrets are resolved at runtime.
      }
      if (auto max_pending = ctx.get(max_pending_arg); max_pending) {
        if (*max_pending == 0) {
          diagnostic::error("`_max_pending` must be greater than zero")
            .primary(
              ctx.get_location(max_pending_arg).value_or(location::unknown))
            .emit(ctx);
        }
        if (*max_pending
            > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
          diagnostic::error("`_max_pending` must fit into a 32-bit integer")
            .primary(
              ctx.get_location(max_pending_arg).value_or(location::unknown))
            .emit(ctx);
        }
      }
      if (auto tls_val = ctx.get(tls_arg)) {
        auto tls
          = tls_options{*tls_val, {.tls_default = true, .is_server = false}};
        if (auto valid = tls.validate(ctx); not valid) {
          return {};
        }
      }
      if (auto auth_val = ctx.get(auth_arg)) {
        if (not validate_auth_record(Option<located<data>>{*auth_val}, ctx)) {
          return {};
        }
      }
      return {};
    });
    // Every instance opens its own NATS connection and JetStream context.
    // With multiple instances publishing to the same subject, messages from
    // different instances interleave, so subscribers may observe events out
    // of input order. Parallelism waives that ordering by design.
    d.parallelizable();
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nats::ToNatsPlugin)
