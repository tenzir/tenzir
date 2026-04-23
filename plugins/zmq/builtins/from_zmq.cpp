//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "zmq/transport.hpp"

#include <tenzir/async.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <fmt/format.h>
#include <folly/coro/BoundedQueue.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <variant>

using namespace std::chrono_literals;

namespace tenzir::plugins::zmq {

namespace {

constexpr auto message_queue_capacity = uint32_t{16};
constexpr auto control_queue_capacity = uint32_t{1};
constexpr auto receive_timeout = 100ms;

auto render_caf_error(const caf::error& err) -> std::string {
  return fmt::to_string(err);
}

auto render_socket_error(const transport::Socket& socket, const caf::error& err)
  -> std::string {
  if (not socket.last_error().empty()) {
    return socket.last_error();
  }
  return render_caf_error(err);
}

struct SourceArgs {
  located<std::string> endpoint;
  Option<ast::expression> prefix;
  bool keep_prefix = false;
  located<ir::pipeline> parser;
};

struct StartupError {
  std::string message;
};

struct ReceiveError {
  std::string message;
};

struct ReadLoopFinished {};

using SourceMessage = variant<StartupError, ReceiveError, ReadLoopFinished,
                              chunk_ptr>;
using MessageQueue = folly::coro::BoundedQueue<SourceMessage>;
using ControlQueue = folly::coro::BoundedQueue<std::monostate>;

struct Runtime {
  explicit Runtime(transport::Socket socket) : socket{std::move(socket)} {
  }

  transport::Socket socket;
};

template <transport::ConnectionMode Mode>
class ZmqSource final : public Operator<void, table_slice> {
public:
  explicit ZmqSource(SourceArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    endpoint_ = transport::normalize_endpoint(args_.endpoint.inner);
    if (args_.prefix) {
      auto prefix = const_eval(*args_.prefix, ctx.dh());
      if (not prefix) {
        request_stop();
        co_return;
      }
      auto* str = try_as<std::string>(&*prefix);
      if (not str) {
        co_await message_queue_->enqueue(
          SourceMessage{StartupError{"`prefix` must be a constant string"}});
        co_return;
      }
      prefix_ = *str;
    }
    auto runtime = std::make_shared<Runtime>(
      transport::Socket{transport::SocketRole::subscriber});
    if (auto err = runtime->socket.set_subscription_prefix(prefix_); not err) {
      co_await message_queue_->enqueue(SourceMessage{StartupError{fmt::format(
        "failed to configure ZeroMQ subscription for `{}`: {}", endpoint_,
        render_socket_error(runtime->socket, err.error()))}});
      co_return;
    }
    if (auto err = runtime->socket.open(Mode, endpoint_); not err) {
      co_await message_queue_->enqueue(SourceMessage{StartupError{fmt::format(
        "failed to open ZeroMQ socket for `{}`: {}", endpoint_,
        render_socket_error(runtime->socket, err.error()))}});
      co_return;
    }
    read_loop_finished_ = false;
    ctx.spawn_task(read_loop(runtime, message_queue_, control_queue_));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    auto* message = result.try_as<SourceMessage>();
    if (not message) {
      co_return;
    }
    co_await co_match(
      std::move(*message),
      [&](StartupError error) -> Task<void> {
        request_stop();
        diagnostic::error("{}", error.message)
          .primary(args_.endpoint.source)
          .emit(ctx);
        co_return;
      },
      [&](ReceiveError error) -> Task<void> {
        diagnostic::error("failed to receive ZeroMQ message")
          .primary(args_.endpoint.source)
          .note("{}", error.message)
          .emit(ctx);
        request_stop();
        co_return;
      },
      [&](ReadLoopFinished) -> Task<void> {
        read_loop_finished_ = true;
        co_return;
      },
      [&](chunk_ptr payload) -> Task<void> {
        if (stop_requested_) {
          co_return;
        }
        if (not args_.keep_prefix and not prefix_.empty()) {
          auto stripped = transport::strip_prefix(std::move(payload), prefix_);
          if (not stripped) {
            diagnostic::warning("failed to strip ZeroMQ prefix")
              .primary(args_.endpoint.source)
              .note("{}", render(stripped.error()))
              .emit(ctx);
            co_return;
          }
          payload = std::move(*stripped);
        }
        auto parser = args_.parser.inner;
        if (not parser.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
          request_stop();
          co_return;
        }
        TENZIR_ASSERT(next_sub_id_ <= static_cast<uint64_t>(
                        std::numeric_limits<int64_t>::max()));
        auto key = data{int64_t{static_cast<int64_t>(next_sub_id_++)}};
        co_await ctx.spawn_sub<chunk_ptr>(key, std::move(parser));
        auto sub = ctx.get_sub(make_view(key));
        if (not sub) {
          request_stop();
          co_return;
        }
        ++active_parsers_;
        auto& sub_pipeline = as<SubHandle<chunk_ptr>>(*sub);
        auto push_result = co_await sub_pipeline.push(std::move(payload));
        if (push_result.is_err()) {
          request_stop();
        }
        co_await sub_pipeline.close();
      });
  }

  auto process_sub(SubKeyView, table_slice slice, Push<table_slice>& push,
                   OpCtx&) -> Task<void> override {
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(active_parsers_ > 0);
    --active_parsers_;
    co_return;
  }

  auto stop(OpCtx&) -> Task<void> override {
    request_stop();
    co_return;
  }

  auto state() -> OperatorState override {
    return is_done() ? OperatorState::done : OperatorState::unspecified;
  }

private:
  auto request_stop() -> void {
    if (stop_requested_) {
      return;
    }
    stop_requested_ = true;
    static_cast<void>(control_queue_->try_enqueue(std::monostate{}));
  }

  auto is_done() const -> bool {
    return stop_requested_ and active_parsers_ == 0 and read_loop_finished_;
  }

  static auto read_loop(std::shared_ptr<Runtime> runtime,
                        std::shared_ptr<MessageQueue> message_queue,
                        std::shared_ptr<ControlQueue> control_queue)
    -> Task<void> {
    while (not control_queue->try_dequeue()) {
      auto message = co_await spawn_blocking([runtime] {
        return runtime->socket.receive(receive_timeout);
      });
      if (message) {
        co_await message_queue->enqueue(SourceMessage{std::move(*message)});
        continue;
      }
      if (message == ec::timeout) {
        continue;
      }
      co_await message_queue->enqueue(SourceMessage{
        ReceiveError{render_socket_error(runtime->socket, message.error())}});
      break;
    }
    co_await message_queue->enqueue(SourceMessage{ReadLoopFinished{}});
  }

  SourceArgs args_;
  std::string endpoint_;
  std::string prefix_;
  std::shared_ptr<MessageQueue> message_queue_
    = std::make_shared<MessageQueue>(message_queue_capacity);
  std::shared_ptr<ControlQueue> control_queue_
    = std::make_shared<ControlQueue>(control_queue_capacity);
  bool stop_requested_ = false;
  bool read_loop_finished_ = true;
  size_t active_parsers_ = 0;
  uint64_t next_sub_id_ = 0;
};

template <transport::ConnectionMode Mode>
class ZmqSourcePlugin : public virtual OperatorPlugin {
public:
  explicit ZmqSourcePlugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto describe() const -> Description override {
    auto d = Describer<SourceArgs, ZmqSource<Mode>>{};
    auto endpoint_arg = d.positional("endpoint", &SourceArgs::endpoint);
    auto prefix_arg = d.named("prefix", &SourceArgs::prefix, "string");
    auto keep_prefix_arg = d.named("keep_prefix", &SourceArgs::keep_prefix);
    auto parser_arg = d.pipeline(&SourceArgs::parser);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TENZIR_UNUSED(keep_prefix_arg);
      TRY(auto endpoint, ctx.get(endpoint_arg));
      if (endpoint.inner.empty()) {
        diagnostic::error("endpoint must not be empty")
          .primary(endpoint.source)
          .emit(ctx);
      }
      if (auto prefix = ctx.get(prefix_arg)) {
        auto noop_dh = null_diagnostic_handler{};
        auto value = const_eval(*prefix, noop_dh);
        if (not value or not is<std::string>(*value)) {
          diagnostic::error("`prefix` must be a constant string")
            .primary(prefix->get_location())
            .emit(ctx);
        }
      }
      TRY(auto parser, ctx.get(parser_arg));
      auto output = parser.inner.infer_type(tag_v<chunk_ptr>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->template is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(parser.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

private:
  std::string name_;
};

class FromZmqPlugin final
  : public ZmqSourcePlugin<transport::ConnectionMode::connect> {
public:
  FromZmqPlugin() : ZmqSourcePlugin{"from_zmq"} {
  }
};

class AcceptZmqPlugin final
  : public ZmqSourcePlugin<transport::ConnectionMode::bind> {
public:
  AcceptZmqPlugin() : ZmqSourcePlugin{"accept_zmq"} {
  }
};

} // namespace

} // namespace tenzir::plugins::zmq

TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::FromZmqPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::zmq::AcceptZmqPlugin)
