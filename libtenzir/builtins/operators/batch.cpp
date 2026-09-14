//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/concept/parseable/numeric/integral.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <folly/coro/Sleep.h>

namespace tenzir::plugins::batch {

namespace {

struct buffer_entry {
  std::vector<table_slice> events;
  uint64_t num_buffered = {};
  std::chrono::steady_clock::time_point start_time
    = std::chrono::steady_clock::now();
};

struct BatchArgs {
  uint64_t limit = defaults::import::table_slice_size;
  duration timeout = std::chrono::minutes{1};
  OptimizationArgs<opt::Order> optimization;
};

class Batch final : public Operator<table_slice, table_slice> {
public:
  explicit Batch(BatchArgs args)
    : limit_{args.limit},
      timeout_{args.timeout},
      order_{args.optimization.order} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // for ordered batching, on schema change, push the current buffer
    if (order_ == EventOrder::ordered and not buffers_.empty()
        and buffers_.begin()->first != input.schema()) {
      TENZIR_ASSERT(buffers_.size() == 1);
      auto& entry = buffers_.begin()->second;
      TENZIR_ASSERT(entry.num_buffered < limit_);
      co_await push(concatenate(std::move(entry.events)));
      buffers_.clear();
    }
    // get buffer and append
    auto [it, _] = buffers_.try_emplace(input.schema());
    auto& entry = it->second;
    entry.num_buffered += input.rows();
    entry.events.push_back(std::move(input));
    // if the buffer hit the limit, push until its below again
    while (entry.num_buffered >= limit_) {
      auto [lhs, rhs] = split(entry.events, limit_);
      auto result = concatenate(std::move(lhs));
      entry.num_buffered -= result.rows();
      entry.start_time = std::chrono::steady_clock::now();
      co_await push(std::move(result));
      entry.events = std::move(rhs);
    }
    if (entry.num_buffered == 0) {
      buffers_.erase(it);
    }
    update_next_timeout();
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (not next_timeout_) {
      co_await buffer_ready_->wait();
    }
    if (next_timeout_) {
      co_await sleep_until(*next_timeout_);
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    co_await flush_timed_out(push);
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_await flush_all(push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_await flush_all(push);
    co_return FinalizeBehavior::done;
  }

private:
  auto update_next_timeout() -> void {
    auto was_null = next_timeout_.is_none();
    next_timeout_ = None{};
    for (const auto& [_, entry] : buffers_) {
      auto deadline = entry.start_time + timeout_;
      if (not next_timeout_ or deadline < *next_timeout_) {
        next_timeout_ = deadline;
      }
    }
    if (was_null and next_timeout_) {
      buffer_ready_->notify_one();
    }
  }

  auto flush_timed_out(Push<table_slice>& push) -> Task<void> {
    const auto now = std::chrono::steady_clock::now();
    for (auto it = buffers_.begin(); it != buffers_.end();) {
      if (now - it->second.start_time >= timeout_) {
        co_await push(concatenate(std::exchange(it->second.events, {})));
        it = buffers_.erase(it);
      } else {
        ++it;
      }
    }
    update_next_timeout();
  }

  auto flush_all(Push<table_slice>& push) -> Task<void> {
    // collect
    std::vector<buffer_entry> to_flush;
    to_flush.reserve(buffers_.size());
    for (auto& [_, entry] : buffers_) {
      to_flush.emplace_back(std::move(entry));
    }
    buffers_.clear();
    update_next_timeout();

    // sort by start time for consistent output ordering
    std::ranges::sort(to_flush, {}, &buffer_entry::start_time);
    for (auto& entry : to_flush) {
      co_await push(concatenate(std::move(entry.events)));
    }
  }

  uint64_t limit_;
  duration timeout_;
  EventOrder order_ = EventOrder::ordered;
  std::unordered_map<type, buffer_entry> buffers_;
  mutable Option<std::chrono::steady_clock::time_point> next_timeout_;
  mutable std::unique_ptr<Notify> buffer_ready_ = std::make_unique<Notify>();
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "batch";
  }

  auto describe() const -> Description override {
    auto d = Describer<BatchArgs, Batch>{};
    auto limit = d.optional_positional("limit", &BatchArgs::limit);
    auto timeout = d.named_optional("timeout", &BatchArgs::timeout);
    d.optimization(&BatchArgs::optimization);
    d.validate([limit, timeout](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(limit)) {
        if (*value == 0) {
          diagnostic::error("batch size must not be 0")
            .primary(ctx.get_location(limit).value())
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(timeout)) {
        if (*value <= duration::zero()) {
          diagnostic::error("timeout must be a positive duration")
            .primary(ctx.get_location(timeout).value())
            .emit(ctx);
        }
      }
      return {};
    });
    // `batch` regroups events without changing, reordering, or filtering
    // them, so downstream hints pass through. Note that the user-provided
    // `limit` argument is a batch size, not an output limit, so we never emit
    // a fresh limit of our own.
    return d.transparent();
  }
};
} // namespace

} // namespace tenzir::plugins::batch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::batch::plugin)
