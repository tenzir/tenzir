//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/async/stream.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/substitute_ctx.hpp"

#include <folly/coro/Retry.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/io/coro/Transport.h>

#include <limits>

namespace tenzir {

template <class Impl>
class StreamTo final : public Operator<table_slice, void> {
public:
  using Args = typename Impl::Args;

  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit StreamTo(Args args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (not co_await impl_.prepare(ctx)) {
      finish();
      co_return;
    }
    evb_ = folly::getGlobalIOExecutor()->getEventBase();
    TENZIR_ASSERT(evb_);
    auto pipeline = std::move(impl_.printer().inner);
    if (not pipeline.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      finish();
      co_return;
    }
    events_write_counter_
      = ctx.make_counter(impl_.events_metric_label(), MetricsDirection::write,
                         MetricsVisibility::external_, MetricsUnit::events);
    if (auto label = impl_.bytes_metric_label(None{})) {
      bytes_write_counter_
        = ctx.make_counter(*label, MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::bytes);
    }
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

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await write_chunk(chunk, ctx);
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

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    co_await finish_gracefully(ctx);
    co_return;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
  }

private:
  auto close_current_transport() -> void {
    if (transport_) {
      auto old_transport = std::move(*transport_);
      transport_ = None{};
      close_stream_transport(std::move(old_transport));
    }
  }

  auto finish_gracefully(OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    lifecycle_ = Lifecycle::done;
    if (not transport_) {
      co_return;
    }
    auto* transport_evb = transport_->getEventBase();
    TENZIR_ASSERT(transport_evb);
    co_await folly::coro::co_withExecutor(
      transport_evb, [this]() -> Task<void> {
        if (auto* transport = transport_->getTransport()) {
          transport->shutdownWrite();
        }
        co_return;
      }());
    try {
      while (co_await folly::coro::co_withExecutor(
        transport_evb, read_stream_chunk(*transport_, buffer_size,
                                         graceful_close_drain_timeout))) {
      }
    } catch (folly::AsyncSocketException const&) {
      // This is a best-effort drain for peers that send data to a sink before
      // reading. Timeouts or close races should not turn a successful sink into
      // a failed pipeline.
    }
    close_current_transport();
  }

  auto ensure_connected(OpCtx& ctx) -> Task<void> {
    if (lifecycle_ == Lifecycle::done or transport_) {
      co_return;
    }
    auto const max_retry_count
      = impl_.max_retry_count()
          ? detail::narrow<uint32_t>(impl_.max_retry_count()->inner)
          : default_connect_max_retry_count;
    try {
      transport_ = co_await folly::coro::retryWithExponentialBackoff(
        max_retry_count, connect_initial_backoff, connect_max_backoff,
        connect_retry_jitter,
        [this, &ctx]() -> Task<folly::coro::Transport> {
          try {
            co_return co_await impl_.connect(evb_);
          } catch (folly::AsyncSocketException const& ex) {
            if (not impl_.max_retry_count()) {
              impl_.emit_connect_warning(ex, ctx.dh());
            }
            throw;
          }
        },
        should_retry_socket);
    } catch (folly::AsyncSocketException const& ex) {
      impl_.emit_connect_error(ex, max_retry_count, ctx.dh());
      finish();
      co_return;
    }
    if (auto label = impl_.bytes_metric_label(*transport_)) {
      bytes_write_counter_
        = ctx.make_counter(*label, MetricsDirection::write,
                           MetricsVisibility::external_, MetricsUnit::bytes);
    }
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
      auto* transport_evb = transport_->getEventBase();
      TENZIR_ASSERT(transport_evb);
      try {
        co_await folly::coro::co_withExecutor(transport_evb,
                                              transport_->write(data));
        bytes_write_counter_.add(chunk->size());
        co_return;
      } catch (folly::AsyncSocketException const& ex) {
        impl_.emit_write_warning(ex, ctx.dh());
      }
      close_current_transport();
    }
  }

  auto finish() -> void {
    lifecycle_ = Lifecycle::done;
    close_current_transport();
  }

  static constexpr auto connect_initial_backoff
    = std::chrono::milliseconds{100};
  static constexpr auto connect_max_backoff = std::chrono::seconds{5};
  static constexpr auto connect_retry_jitter = 0.0;
  static constexpr auto default_connect_max_retry_count
    = std::numeric_limits<uint32_t>::max();
  static constexpr auto buffer_size = size_t{64 * 1024};
  static constexpr auto graceful_close_drain_timeout
    = std::chrono::milliseconds{10};
  Impl impl_;
  data sub_key_ = data{int64_t{0}};
  folly::EventBase* evb_ = nullptr;
  Option<folly::coro::Transport> transport_;
  MetricsCounter bytes_write_counter_;
  MetricsCounter events_write_counter_;
  Lifecycle lifecycle_ = Lifecycle::running;
};

} // namespace tenzir
