//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/semaphore.hpp>
#include <tenzir/async/uds.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/CancellationToken.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/coro/WithCancellation.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <filesystem>
#include <limits>
#include <memory>

namespace tenzir::plugins::serve_uds {

namespace {

constexpr auto listen_backlog = uint32_t{128};
constexpr auto accept_retry_delay = std::chrono::milliseconds{100};
constexpr auto message_queue_capacity = uint32_t{512};

struct ServeUdsArgs {
  located<std::string> path;
  Option<located<uint64_t>> max_connections;
  located<ir::pipeline> printer;
};

class ServeUds final : public Operator<table_slice, void> {
public:
  struct Accepted {
    Box<folly::coro::Transport> client;
  };

  struct Payload {
    chunk_ptr chunk;
  };

  struct AcceptLoopFinished {};

  struct PrinterFinished {};

  using Message
    = variant<Accepted, Payload, AcceptLoopFinished, PrinterFinished>;
  using MessageQueue = folly::coro::BoundedQueue<Message>;

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ServeUds(ServeUdsArgs args)
    : args_{std::move(args)},
      max_connections_{args_.max_connections ? args_.max_connections->inner
                                             : uint64_t{128}},
      connection_slots_{detail::narrow<size_t>(max_connections_)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      co_await request_stop();
      co_return;
    }
    if (not prepare_uds_listen_path(path_, args_.path.source, ctx.dh())) {
      co_await request_stop();
      co_return;
    }
    address_ = std::move(*address);
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto socket = folly::AsyncServerSocket::newSocket(evb_);
    server_.emplace(
      std::in_place,
      Arc<folly::AsyncServerSocket>::from_non_null(std::move(socket)), address_,
      listen_backlog);
    should_cleanup_socket_ = true;
    accept_loop_finished_ = false;
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "serve_uds"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "serve_uds"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    ctx.spawn_task([this, &ctx]() -> Task<void> {
      auto notify_finished = detail::scope_guard{[this, &ctx]() noexcept {
        ctx.spawn_task([this]() -> Task<void> {
          co_await message_queue_->enqueue(AcceptLoopFinished{});
        });
      }};
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        accept_cancel_->getToken());
      co_await folly::coro::co_withCancellation(token, accept_loop(ctx));
    });
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      co_await request_stop();
      co_return;
    }
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* message_ptr = result.try_as<Message>();
    if (not message_ptr) {
      co_return;
    }
    auto message = std::move(*message_ptr);
    co_await co_match(
      std::move(message),
      [&](Accepted accepted) -> Task<void> {
        if (lifecycle_ != Lifecycle::running) {
          close_client(std::move(accepted.client));
          release_connection_slot();
          maybe_finish_draining();
          co_return;
        }
        TENZIR_DEBUG("serve_uds: accepted client on {}", path_);
        clients_.push_back(std::move(accepted.client));
        co_return;
      },
      [&](Payload payload) -> Task<void> {
        if (lifecycle_ == Lifecycle::done or not payload.chunk
            or payload.chunk->size() == 0 or clients_.empty()) {
          co_return;
        }
        co_await broadcast_payload(payload.chunk, ctx.dh());
      },
      [&](AcceptLoopFinished) -> Task<void> {
        accept_loop_finished_ = true;
        maybe_finish_draining();
        co_return;
      },
      [&](PrinterFinished) -> Task<void> {
        co_await request_stop();
        co_return;
      });
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto const rows = input.rows();
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      co_await request_stop();
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      co_await request_stop();
      co_return;
    }
    events_write_counter_.add(rows);
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done or not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(Payload{std::move(chunk)});
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      begin_draining();
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
      } else {
        co_await request_stop();
      }
    }
    maybe_finish_draining();
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    co_await message_queue_->enqueue(PrinterFinished{});
    co_return;
  }

  auto state() -> OperatorState override {
    maybe_finish_draining();
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  static auto close_client(Box<folly::coro::Transport> client) -> void {
    auto* evb = client->getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([client = std::move(client)]() mutable {
      client->close();
    });
  }

  auto stop_accepting() -> void {
    accept_cancel_->requestCancellation();
    if (server_ and evb_) {
      evb_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
        if (server_) {
          (*server_)->close();
        }
      });
    }
    cleanup_socket_path();
  }

  auto cleanup_socket_path() -> void {
    if (not should_cleanup_socket_) {
      return;
    }
    should_cleanup_socket_ = false;
    auto ec = std::error_code{};
    std::filesystem::remove(path_, ec);
  }

  auto begin_draining() -> void {
    if (lifecycle_ != Lifecycle::running) {
      return;
    }
    lifecycle_ = Lifecycle::draining;
    stop_accepting();
  }

  auto request_stop() -> Task<void> {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    begin_draining();
    close_all_clients();
    maybe_finish_draining();
  }

  auto maybe_finish_draining() -> void {
    if (lifecycle_ != Lifecycle::draining) {
      return;
    }
    if (accept_loop_finished_
        and static_cast<uint64_t>(connection_slots_.available_permits())
              == max_connections_) {
      lifecycle_ = Lifecycle::done;
      cleanup_socket_path();
    }
  }

  auto release_connection_slot() -> void {
    connection_slots_.add_permit();
  }

  auto close_all_clients() -> void {
    for (auto& client : clients_) {
      close_client(std::move(client));
      release_connection_slot();
    }
    clients_.clear();
  }

  auto write_to_client(Box<folly::coro::Transport>& client,
                       folly::ByteRange data) -> Task<bool> {
    auto* client_evb = client->getEventBase();
    TENZIR_ASSERT(client_evb);
    try {
      co_await folly::coro::co_withExecutor(client_evb, client->write(data));
      bytes_write_counter_.add(data.size());
      co_return true;
    } catch (folly::AsyncSocketException const&) {
      co_return false;
    }
  }

  auto broadcast_payload(chunk_ptr const& chunk, diagnostic_handler& dh)
    -> Task<void> {
    TENZIR_UNUSED(dh);
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    for (size_t i = 0; i < clients_.size();) {
      auto ok = co_await write_to_client(clients_[i], data);
      if (ok) {
        ++i;
        continue;
      }
      close_client(std::move(clients_[i]));
      release_connection_slot();
      clients_.erase(clients_.begin() + i);
      maybe_finish_draining();
    }
  }

  auto finish_accept(Box<folly::coro::Transport> client) -> Task<void> {
    auto release_connection_slot_guard = detail::scope_guard{[this]() noexcept {
      release_connection_slot();
    }};
    TENZIR_DEBUG("serve_uds: accepted client on {}", path_);
    co_await message_queue_->enqueue(Accepted{std::move(client)});
    release_connection_slot_guard.disable();
  }

  auto accept_loop(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(server_);
    TENZIR_DEBUG("serve_uds: accept loop started on {}", path_);
    auto should_retry_accept = [](folly::exception_wrapper const& ew) {
      return ew.is_compatible_with<folly::AsyncSocketException>();
    };
    while (true) {
      co_await connection_slots_.consume();
      auto release_connection_slot_guard
        = detail::scope_guard{[this]() noexcept {
            release_connection_slot();
          }};
      auto transport = co_await folly::coro::retryWithExponentialBackoff(
        std::numeric_limits<uint32_t>::max(), accept_retry_delay,
        accept_retry_delay, 0.0,
        [this, &ctx]() -> Task<Box<folly::coro::Transport>> {
          try {
            co_return co_await folly::coro::co_withExecutor(
              evb_, (*server_)->accept());
          } catch (folly::AsyncSocketException const& ex) {
            diagnostic::warning("failed to accept incoming connection")
              .primary(args_.path.source)
              .note("path: {}", path_)
              .note("reason: {}", ex.what())
              .emit(ctx.dh());
            throw;
          }
        },
        should_retry_accept);
      ctx.spawn_task(finish_accept(std::move(transport)));
      release_connection_slot_guard.disable();
    }
  }

  ServeUdsArgs args_;
  data sub_key_ = data{int64_t{0}};
  std::string path_;
  folly::SocketAddress address_;
  folly::EventBase* evb_ = nullptr;
  Option<Box<UdsServerSocket>> server_;
  uint64_t max_connections_ = 128;
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Semaphore connection_slots_;
  Box<folly::CancellationSource> accept_cancel_{std::in_place};
  std::vector<Box<folly::coro::Transport>> clients_;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  bool accept_loop_finished_ = true;
  bool should_cleanup_socket_ = false;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class ServeUdsPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.serve_uds";
  }

  auto describe() const -> Description override {
    auto d = Describer<ServeUdsArgs, ServeUds>{};
    d.positional("path", &ServeUdsArgs::path);
    auto max_connections_arg
      = d.named("max_connections", &ServeUdsArgs::max_connections);
    auto printer_arg
      = d.pipeline(&ServeUdsArgs::printer, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_connections = ctx.get(max_connections_arg);
          max_connections) {
        auto loc
          = ctx.get_location(max_connections_arg).value_or(location::unknown);
        if (max_connections->inner == 0) {
          diagnostic::error("max_connections must be greater than 0")
            .primary(loc)
            .emit(ctx);
        } else if (max_connections->inner > static_cast<uint64_t>(
                     std::numeric_limits<size_t>::max())) {
          diagnostic::error("max_connections is too large")
            .primary(loc)
            .note("maximum supported value: {}",
                  std::numeric_limits<size_t>::max())
            .emit(ctx);
        }
      }
      TRY(auto printer, ctx.get(printer_arg));
      auto output = printer.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(printer.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::serve_uds

TENZIR_REGISTER_PLUGIN(tenzir::plugins::serve_uds::ServeUdsPlugin)
