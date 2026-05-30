//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/async/uds.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/file.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/SocketAddress.h>
#include <folly/String.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::to_uds {

namespace {

constexpr auto connect_timeout = std::chrono::seconds{5};
constexpr auto connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto connect_max_backoff = std::chrono::seconds{5};
constexpr auto connect_retry_jitter = 0.0;
constexpr auto default_connect_max_retry_count
  = std::numeric_limits<uint32_t>::max();
constexpr auto message_queue_capacity = uint32_t{10};

constexpr auto should_retry_connect = [](folly::exception_wrapper const& ew) {
  return ew.is_compatible_with<folly::AsyncSocketException>();
};

struct ToUdsArgs {
  located<std::string> path;
  Option<located<uint64_t>> max_retry_count;
  located<ir::pipeline> printer;
};

auto describe_socket_error(folly::AsyncSocketException const& ex)
  -> std::string {
  if (auto err = ex.getErrno(); err > 0) {
    return folly::errnoStr(err);
  }
  return ex.what();
}

class ToUds final : public Operator<table_slice, void> {
public:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ToUds(ToUdsArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    path_ = expand_home(args_.path.inner);
    auto address = make_uds_socket_address(path_, args_.path.source, ctx.dh());
    if (not address) {
      finish();
      co_return;
    }
    address_ = std::move(*address);
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto pipeline = std::move(args_.printer.inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      finish();
      co_return;
    }
    events_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_uds"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::events);
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_uds"},
                         MetricsDirection::write, MetricsVisibility::external_,
                         MetricsUnit::bytes);
    co_await ctx.spawn_sub<table_slice>(sub_key_, std::move(pipeline));
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(make_view(sub_key_));
    if (not sub) {
      finish();
      co_return;
    }
    auto const rows = input.rows();
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      finish();
      co_return;
    }
    events_write_counter_.add(rows);
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(std::move(chunk));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* chunk = result.try_as<chunk_ptr>();
    if (not chunk) {
      co_return;
    }
    if (not *chunk) {
      finish();
      co_return;
    }
    if (lifecycle_ == Lifecycle::done or (*chunk)->size() == 0) {
      co_return;
    }
    co_await write_chunk(*chunk, ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      close_current_transport();
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(make_view(sub_key_))) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
        co_return FinalizeBehavior::continue_;
      }
      finish();
      co_return FinalizeBehavior::done;
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    ctx.spawn_task([this]() -> Task<void> {
      co_await message_queue_->enqueue(chunk_ptr{});
    });
    co_return;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  using MessageQueue = folly::coro::BoundedQueue<chunk_ptr>;

  static auto close_transport(folly::coro::Transport transport) -> void {
    auto* evb = transport.getEventBase();
    TENZIR_ASSERT(evb);
    evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
      transport.close();
    });
  }

  auto close_current_transport() -> void {
    if (transport_) {
      auto old_transport = std::move(*transport_);
      transport_ = None{};
      close_transport(std::move(old_transport));
    }
  }

  auto ensure_connected(OpCtx& ctx) -> Task<void> {
    if (lifecycle_ == Lifecycle::done or transport_) {
      co_return;
    }
    auto const max_retry_count
      = args_.max_retry_count
          ? detail::narrow<uint32_t>(args_.max_retry_count->inner)
          : default_connect_max_retry_count;
    auto emit_final_error = [&](folly::AsyncSocketException const& ex) {
      diagnostic::error("failed to connect to UNIX domain socket: {}",
                        describe_socket_error(ex))
        .primary(args_.path.source)
        .note("path: {}", path_)
        .note("gave up after {} {}", max_retry_count,
              max_retry_count == 1 ? "retry" : "retries")
        .hint("ensure a server is listening on this socket path")
        .emit(ctx.dh());
      finish();
    };
    auto emit_retry_warning = [&](folly::AsyncSocketException const& ex) {
      diagnostic::warning("failed to connect to UNIX domain socket: {}",
                          describe_socket_error(ex))
        .primary(args_.path.source)
        .note("path: {}", path_)
        .hint("ensure a server is listening on this socket path")
        .emit(ctx.dh());
    };
    auto connect = [this]() -> Task<folly::coro::Transport> {
      TENZIR_DEBUG("to_uds: connecting to {}", path_);
      co_return co_await folly::coro::co_withExecutor(
        evb_, folly::coro::Transport::newConnectedSocket(
                evb_, address_,
                std::chrono::duration_cast<std::chrono::milliseconds>(
                  connect_timeout)));
    };
    try {
      transport_ = co_await folly::coro::retryWithExponentialBackoff(
        max_retry_count, connect_initial_backoff, connect_max_backoff,
        connect_retry_jitter,
        [this, &connect,
         &emit_retry_warning]() -> Task<folly::coro::Transport> {
          try {
            co_return co_await connect();
          } catch (folly::AsyncSocketException const& ex) {
            if (not args_.max_retry_count) {
              emit_retry_warning(ex);
            }
            throw;
          }
        },
        should_retry_connect);
    } catch (folly::AsyncSocketException const& ex) {
      emit_final_error(ex);
      co_return;
    }
    TENZIR_DEBUG("to_uds: connected to {}", path_);
  }

  auto write_chunk(chunk_ptr const& chunk, OpCtx& ctx) -> Task<void> {
    auto data = folly::ByteRange{
      reinterpret_cast<unsigned char const*>(chunk->data()),
      chunk->size(),
    };
    while (lifecycle_ != Lifecycle::done) {
      co_await ensure_connected(ctx);
      if (lifecycle_ == Lifecycle::done or not transport_) {
        co_return;
      }
      auto write_error = Option<std::string>{};
      auto* transport_evb = transport_->getEventBase();
      TENZIR_ASSERT(transport_evb);
      try {
        co_await folly::coro::co_withExecutor(transport_evb,
                                              transport_->write(data));
        bytes_write_counter_.add(chunk->size());
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        write_error = ex.what();
      }
      diagnostic::warning("failed to write to UNIX domain socket")
        .primary(args_.path.source)
        .note("path: {}", path_)
        .note("reason: {}", *write_error)
        .note("retrying after reconnect")
        .emit(ctx.dh());
      close_current_transport();
    }
  }

  auto finish() -> void {
    lifecycle_ = Lifecycle::done;
    close_current_transport();
  }

  ToUdsArgs args_;
  data sub_key_ = data{int64_t{0}};
  std::string path_;
  folly::SocketAddress address_;
  folly::EventBase* evb_ = nullptr;
  mutable Box<MessageQueue> message_queue_{
    std::in_place,
    message_queue_capacity,
  };
  Option<folly::coro::Transport> transport_;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  Lifecycle lifecycle_ = Lifecycle::running;
};

class ToUdsPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_uds";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToUdsArgs, ToUds>{};
    d.positional("path", &ToUdsArgs::path);
    auto max_retry_count_arg
      = d.named("max_retry_count", &ToUdsArgs::max_retry_count);
    auto printer_arg
      = d.pipeline(&ToUdsArgs::printer, SubOptimize::from_downstream);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto max_retry_count = ctx.get(max_retry_count_arg)) {
        if (max_retry_count->inner > std::numeric_limits<uint32_t>::max()) {
          diagnostic::error("`max_retry_count` must be <= {}",
                            std::numeric_limits<uint32_t>::max())
            .primary(max_retry_count->source)
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

} // namespace tenzir::plugins::to_uds

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_uds::ToUdsPlugin)
