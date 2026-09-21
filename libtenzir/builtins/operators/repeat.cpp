//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <folly/coro/BoundedQueue.h>

#include <limits>
#include <string>

namespace tenzir::plugins::repeat {

namespace {

struct RepeatArgs {
  uint64_t count = std::numeric_limits<uint64_t>::max();
};

template <class Events>
class Repeat final : public Operator<Events, Events> {
public:
  explicit Repeat(RepeatArgs args) : count_{args.count} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if ((phase_ == Phase::replay_finite or phase_ == Phase::replay_infinite)
        and has_replay_work()) {
      schedule_replay(ctx);
    }
    co_return;
  }

  auto process(Events input, Push<Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // If count is 0, we don't emit anything (handled by state() returning done)
    if (count_ == 0) {
      co_return;
    }
    // Cache slices for repetition.
    if (count_ > 1) {
      buffer_.push_back(input);
    }
    // Always emit the input during first pass
    co_await push(std::move(input));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_return co_await replay_queue_->dequeue();
  }

  auto process_task(Any result, Push<Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    if ((phase_ != Phase::replay_finite and phase_ != Phase::replay_infinite)
        or not has_replay_work()) {
      co_return;
    }
    TENZIR_ASSERT(next_index_ < buffer_.size());
    co_await push(buffer_[next_index_]);
    next_index_ += 1;
    if (next_index_ == buffer_.size()) {
      next_index_ = 0;
      if (phase_ == Phase::replay_finite) {
        TENZIR_ASSERT(remaining_repetitions_ > 0);
        remaining_repetitions_ -= 1;
        if (remaining_repetitions_ == 0) {
          phase_ = Phase::finished;
          co_return;
        }
      }
    }
    schedule_replay(ctx);
  }

  auto finalize(Push<Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (phase_ == Phase::finished or buffer_.empty() or count_ <= 1) {
      phase_ = Phase::finished;
      co_return FinalizeBehavior::done;
    }
    if (phase_ == Phase::input) {
      phase_ = count_ == std::numeric_limits<uint64_t>::max()
                 ? Phase::replay_infinite
                 : Phase::replay_finite;
      next_index_ = 0;
      if (phase_ == Phase::replay_finite) {
        remaining_repetitions_ = count_ - 1;
      }
      schedule_replay(ctx);
    }
    co_return FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    if (count_ == 0 or phase_ == Phase::finished) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // Without an explicit count, `repeat` replays its input forever. A
    // graceful stop must end this otherwise infinite replay. With a finite
    // count, we instead keep draining the remaining repetitions.
    if (count_ == std::numeric_limits<uint64_t>::max()) {
      phase_ = Phase::finished;
    }
    co_return;
  }

  auto snapshot(Serde& serde) -> void override {
    if constexpr (std::same_as<Events, table_slice>) {
      serde("buffer", buffer_);
    } else {
      // Nova event serialization is not implemented yet.
      TENZIR_TODO();
    }
    serde("phase", phase_);
    serde("remaining_repetitions", remaining_repetitions_);
    serde("next_index", next_index_);
  }

private:
  enum class Phase {
    input,
    replay_finite,
    replay_infinite,
    finished,
  };

  struct ReplayTick {};

  auto has_replay_work() const -> bool {
    return (phase_ == Phase::replay_finite and remaining_repetitions_ > 0)
           or (phase_ == Phase::replay_infinite and not buffer_.empty());
  }

  auto schedule_replay(OpCtx& ctx) -> void {
    ctx.spawn_task([queue = replay_queue_]() mutable -> Task<void> {
      co_await queue->enqueue(ReplayTick{});
    });
  }

  friend auto inspect(auto& f, Phase& x) -> bool {
    return f.apply(x);
  }

  using ReplayQueue = folly::coro::BoundedQueue<ReplayTick>;

  uint64_t count_;
  std::vector<Events> buffer_;
  Phase phase_ = Phase::input;
  uint64_t remaining_repetitions_ = 0;
  uint64_t next_index_ = 0;
  mutable Arc<ReplayQueue> replay_queue_{std::in_place, 1};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "repeat";
  }

  auto describe() const -> Description override {
    auto d = Describer<RepeatArgs, Repeat<table_slice>, Repeat<nova::Events>>{};
    d.optional_positional("count", &RepeatArgs::count);
    return d.invariant_order_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::repeat

TENZIR_REGISTER_PLUGIN(tenzir::plugins::repeat::plugin)
