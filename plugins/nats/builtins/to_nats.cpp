//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir_plugins/nats/common.hpp"

#include <tenzir/arc.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>

#include <chrono>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <tuple>
#include <utility>
#include <vector>

namespace tenzir::plugins::nats {

namespace {

using namespace tenzir::si_literals;
using namespace std::chrono_literals;

constexpr auto default_max_pending = uint64_t{1_Ki};
constexpr auto default_stall_wait = 200ms;

auto make_root_field(std::string field) -> ast::root_field {
  return ast::root_field{
    ast::identifier{std::move(field), location::unknown},
  };
}

struct ToNatsArgs {
  located<std::string> subject;
  ast::expression message{make_root_field("message")};
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
};

auto make_publish_ack_error(jsPubAckErr const& error) -> PublishAckError {
  auto reason = fmt::format("{}: {}", nats_status_string(error.Err),
                            error.ErrText ? error.ErrText : "");
  if (error.ErrCode != 0) {
    reason += fmt::format(" (JetStream error code {})",
                          static_cast<int>(error.ErrCode));
  }
  return {.reason = std::move(reason)};
}

struct PublishAckFailures {
  auto record(jsPubAckErr const& error) -> void {
    auto guard = std::lock_guard{mutex};
    ++count;
    if (first_reason.empty()) {
      first_reason = make_publish_ack_error(error).reason;
    }
  }

  auto drain() -> PublishAckError {
    auto guard = std::lock_guard{mutex};
    return {
      .count = std::exchange(count, uint64_t{0}),
      .reason = std::exchange(first_reason, {}),
    };
  }

  std::mutex mutex;
  uint64_t count = 0;
  std::string first_reason;
};

void publish_error_handler(jsCtx*, jsPubAckErr* error, void* closure) {
  auto* failures = static_cast<PublishAckFailures*>(closure);
  TENZIR_ASSERT(failures);
  if (error) {
    failures->record(*error);
  }
}

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
                         MetricsDirection::write, MetricsVisibility::external_);
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
                          ctx.dh(), *evb);
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
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto publish_slice(table_slice input, OpCtx& ctx) -> bool {
    if (done_ or input.rows() == 0 or not js_) {
      return true;
    }
    auto& dh = ctx.dh();
    auto messages = eval(args_.message, input, dh);
    auto headers = std::optional<multi_series>{};
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
      diagnostic::error("{} NATS publish acknowledgment{} failed",
                        failures.count, failures.count == 1 ? "" : "s")
        .primary(args_.subject.source)
        .note("first error: {}", failures.reason)
        .emit(ctx);
    }
  }

  ToNatsArgs args_;
  mutable Arc<PublishAckFailures> ack_failures_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  nats_options_ptr options_;
  nats_connection_ptr connection_;
  js_ctx_ptr js_;
  MetricsCounter write_bytes_counter_;
  bool done_ = false;
};

class ToNatsPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_nats";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToNatsArgs, ToNats>{};
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::nats

TENZIR_REGISTER_PLUGIN(tenzir::plugins::nats::ToNatsPlugin)
