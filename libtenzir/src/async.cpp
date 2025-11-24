//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include "tenzir/async/queue_scope.hpp"
#include "tenzir/async/unbounded_queue.hpp"

#include <folly/Executor.h>
#include <folly/coro/AwaitResult.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

// TODO: Why does this not report line numbers correctly?
#undef TENZIR_UNREACHABLE
#define TENZIR_UNREACHABLE()                                                   \
  TENZIR_ERROR("unreachable");                                                 \
  tenzir::panic("unreachable")

namespace tenzir {

class Pass final : public Operator<table_slice, table_slice> {
public:
  auto process(table_slice input, Push<table_slice>& push, AsyncCtx& ctx)
    -> Task<void> override {
    co_await push(std::move(input));
  }
};

auto filter2(const table_slice& slice, const ast::expression& expr,
             diagnostic_handler& dh, bool warn) -> std::vector<table_slice> {
  auto results = std::vector<table_slice>{};
  auto offset = int64_t{0};
  for (auto& filter : eval(expr, slice, dh)) {
    auto array = try_as<arrow::BooleanArray>(&*filter.array);
    if (not array) {
      diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
        .primary(expr)
        .emit(dh);
      offset += filter.array->length();
      continue;
    }
    if (array->true_count() == array->length()) {
      results.push_back(subslice(slice, offset, offset + array->length()));
      offset += array->length();
      continue;
    }
    if (warn) {
      diagnostic::warning("assertion failure").primary(expr).emit(dh);
    }
    auto length = array->length();
    auto current_value = array->Value(0);
    auto current_begin = int64_t{0};
    // We add an artificial `false` at index `length` to flush.
    for (auto i = int64_t{1}; i < length + 1; ++i) {
      const auto next = i != length && array->IsValid(i) && array->Value(i);
      if (current_value == next) {
        continue;
      }
      if (current_value) {
        results.push_back(subslice(slice, offset + current_begin, offset + i));
      }
      current_value = next;
      current_begin = i;
    }
    offset += length;
  }
  return results;
}

template <class T>
class Receiver {
public:
  explicit Receiver(std::shared_ptr<UnboundedQueue<T>> queue)
    : queue_{std::move(queue)} {
    TENZIR_ASSERT(queue_);
  }

  auto receive() -> Task<T> {
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_DEBUG("CANCELLED");
    }};
    TENZIR_VERBOSE("waiting for queue in receiver ({}): {}",
                   fmt::ptr(queue_.get()),
                   (co_await folly::coro::co_current_cancellation_token)
                     .isCancellationRequested());
    TENZIR_ASSERT(queue_);
    auto result = co_await queue_->dequeue();
    TENZIR_WARN("got item for queue in receiver");
    guard.disable();
    co_return result;
  }

  auto into_generator() && -> AsyncGenerator<T> {
    return folly::coro::co_invoke(
      [self = std::move(*this)] mutable -> AsyncGenerator<T> {
        TENZIR_VERBOSE("starting receiver generator");
        while (true) {
          auto result = co_await self.receive();
          TENZIR_VERBOSE("got item in receiver generator");
          co_yield std::move(result);
          TENZIR_VERBOSE("continuing in result generator");
        }
      });
  }

private:
  std::shared_ptr<UnboundedQueue<T>> queue_;
};

template <class T>
class Sender {
public:
  explicit Sender(std::shared_ptr<UnboundedQueue<T>> queue)
    : queue_{std::move(queue)} {
  }

  auto send(T x) -> void {
    queue_->enqueue(std::move(x));
  }

private:
  std::shared_ptr<UnboundedQueue<T>> queue_;
};

template <class T>
auto make_unbounded_channel() -> std::pair<Sender<T>, Receiver<T>> {
  auto shared = std::make_shared<UnboundedQueue<T>>();
  return {Sender<T>{shared}, Receiver<T>{shared}};
}

template <class T>
class OpPushWrapper final : public Push<T> {
public:
  explicit OpPushWrapper(Push<OperatorMsg<T>>& push) : push_{push} {
  }

  virtual auto operator()(T output) -> Task<void> override {
    return push_(std::move(output));
  }

private:
  Push<OperatorMsg<T>>& push_;
};

template <class T>
OpPushWrapper(Box<Push<OperatorMsg<T>>>&) -> OpPushWrapper<T>;

template <class Input, class Output>
class Runner {
public:
  Runner(Box<Operator<Input, Output>> op,
         Box<Pull<OperatorMsg<Input>>> pull_upstream,
         Box<Push<OperatorMsg<Output>>> push_downstream,
         Receiver<FromControl> from_control, Sender<ToControl> to_control,
         caf::actor_system& sys, diagnostic_handler& dh)
    : op_{std::move(op)},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      ctx_{sys, dh} {
  }

  auto run_to_completion() && -> Task<void> {
    TENZIR_WARN("starting operator runner");
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_WARN("returning from operator runner");
    }};
    co_await queue_.activate(run());
  }

private:
  auto run() -> Task<void> {
    TENZIR_INFO("entering run loop of {}", typeid(*op_).name());
    // co_await folly::coro::co_scope_exit(
    //   [](Runner* self) -> Task<void> {
    //     TENZIR_WARN("shutting down operator {} with {} pending",
    //                 typeid(*self->op_).name(), self->queue_.pending());
    //     // TODO: Can we always do this here?
    //     co_await self->queue_.cancel_and_join();
    //     TENZIR_WARN("shutdown done for {}", typeid(*self->op_).name());
    //   },
    //   this);
    try {
      TENZIR_INFO("-> pre start");
      if constexpr (std::same_as<Output, void>) {
        co_await op_->start(ctx_);
      } else {
        auto push = OpPushWrapper{push_downstream_};
        co_await op_->start(push, ctx_);
      }
      TENZIR_INFO("-> post start");
      queue_.spawn(op_->await_task());
      queue_.spawn(pull_upstream_());
      queue_.spawn(from_control_.receive());
      while (not got_shutdown_request_) {
        co_await folly::coro::co_safe_point;
        co_await tick();
      }
    } catch (folly::OperationCancelled) {
      TENZIR_VERBOSE("shutting down operator after cancellation");
      throw;
    } catch (std::exception& e) {
      TENZIR_ERROR("shutting down operator after uncaught exception: {}",
                   e.what());
      throw;
    } catch (...) {
      TENZIR_ERROR("shutting down operator after uncaught exception");
      throw;
    }
    queue_.cancel();
  }

  auto tick() -> Task<void> {
    ticks_ += 1;
    TENZIR_INFO("tick {} in {}", ticks_, typeid(*op_).name());
    switch (op_->state()) {
      case OperatorState::done:
        co_await handle_done();
        break;
      case OperatorState::unspecified:
        break;
    }
    TENZIR_VERBOSE("waiting in {} for message", typeid(*op_).name());
    auto message = check(co_await queue_.next());
    co_await match(std::move(message), [&](auto message) {
      return process(std::move(message));
    });
  }

  auto process(std::any message) -> Task<void> {
    // The task provided by the inner implementation completed.
    TENZIR_VERBOSE("got future result in {}", typeid(*op_).name());
    if constexpr (std::same_as<Output, void>) {
      co_await op_->process_task(std::move(message), ctx_);
    } else {
      auto push = OpPushWrapper{push_downstream_};
      co_await op_->process_task(std::move(message), push, ctx_);
    }
    if (op_->state() == OperatorState::done) {
      co_await handle_done();
    } else {
      queue_.spawn(op_->await_task());
    }
    TENZIR_VERBOSE("handled future result in {}", typeid(*op_).name());
  }

  auto process(OperatorMsg<Input> message) -> Task<void> {
    co_await match(
      std::move(message),
      // The template indirection is necessary to prevent a `void` parameter.
      [&]<std::same_as<Input> Input2>(Input2 input) -> Task<void> {
        if constexpr (std::same_as<Input, void>) {
          TENZIR_UNREACHABLE();
        } else {
          TENZIR_VERBOSE("got input in {}", typeid(*op_).name());
          if (is_done_) {
            // No need to forward the input.
            co_return;
          }
          if constexpr (std::same_as<Output, void>) {
            co_await op_->process(input, ctx_);
          } else {
            auto push = OpPushWrapper{push_downstream_};
            co_await op_->process(input, push, ctx_);
          }
        }
      },
      [&](Signal signal) -> Task<void> {
        switch (signal) {
          case Signal::end_of_data:
            TENZIR_VERBOSE("got end of data in {}", typeid(*op_).name());
            if constexpr (std::same_as<Input, void>) {
              TENZIR_UNREACHABLE();
            } else {
              // TODO: The default behavior is to transition to done?
              co_await handle_done();
            }
            co_return;
          case Signal::checkpoint:
            TENZIR_VERBOSE("got checkpoint in {}", typeid(*op_).name());
            co_await op_->checkpoint();
            co_await push_downstream_(Signal::checkpoint);
            co_return;
        }
        TENZIR_UNREACHABLE();
      });
    queue_.spawn(pull_upstream_());
  }

  auto process(FromControl message) -> Task<void> {
    co_await match(
      std::move(message),
      [&](PostCommit) -> Task<void> {
        TENZIR_VERBOSE("got post commit in {}", typeid(*op_).name());
        co_await op_->post_commit();
      },
      [&](Shutdown) -> Task<void> {
        // FIXME: Cleanup on shutdown?
        TENZIR_VERBOSE("got shutdown in {}", typeid(*op_).name());
        got_shutdown_request_ = true;
        co_return;
      },
      [&](StopOutput) -> Task<void> {
        co_await handle_done();
      });
    queue_.spawn(from_control_.receive());
  }

  auto handle_done() -> Task<void> {
    // We want to run this code once.
    if (is_done_) {
      co_return;
    }
    is_done_ = true;
    TENZIR_VERBOSE("...");
    // Immediately inform control that we want no more data.
    if constexpr (not std::same_as<Input, void>) {
      to_control_.send(ToControl::no_more_input);
    }
    // Then finalize the operator, which can still produce output.
    if constexpr (std::same_as<Output, void>) {
      co_await op_->finalize(ctx_);
    } else {
      auto push = OpPushWrapper{push_downstream_};
      co_await op_->finalize(push, ctx_);
      co_await push_downstream_(Signal::end_of_data);
    }
    TENZIR_WARN("sending ready to shutdown");
    to_control_.send(ToControl::ready_for_shutdown);
  }

  Box<Operator<Input, Output>> op_;
  Box<Pull<OperatorMsg<Input>>> pull_upstream_;
  Box<Push<OperatorMsg<Output>>> push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  AsyncCtx ctx_;

  QueueScope<variant<std::any, OperatorMsg<Input>, FromControl>> queue_;
  bool got_shutdown_request_ = false;
  bool is_done_ = false;
  // TODO: Expose this?
  std::atomic<size_t> ticks_ = 0;
};

namespace {

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op,
                  Box<Pull<OperatorMsg<Input>>> pull_upstream,
                  Box<Push<OperatorMsg<Output>>> push_downstream,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await Runner<Input, Output>{
    std::move(op),
    std::move(pull_upstream),
    std::move(push_downstream),
    std::move(from_control),
    std::move(to_control),
    sys,
    dh,
  }
    .run_to_completion();
}

} // namespace

template <class Input, class Output>
class ChainRunner {
public:
  ChainRunner(OperatorChain<Input, Output> chain,
              Box<Pull<OperatorMsg<Input>>> pull_upstream,
              Box<Push<OperatorMsg<Output>>> push_downstream,
              Receiver<FromControl> from_control, Sender<ToControl> to_control,
              caf::actor_system& sys, diagnostic_handler& dh)
    : operators_{std::move(chain).unwrap()},
      pull_upstream_{std::move(pull_upstream)},
      push_downstream_{std::move(push_downstream)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      sys_{sys},
      dh_{dh} {
  }

  auto run_to_completion() && -> Task<void> {
    return queue_.activate([&] -> Task<void> {
      spawn_operators();
      co_await run_until_shutdown();
      TENZIR_WARN("cancelling all queue items in chain");
      queue_.cancel();
      TENZIR_WARN("waiting for chain queue tasks to finish");
    });
  }

private:
  auto spawn_operators() -> void {
    TENZIR_WARN("beginning chain setup");
    auto next_input
      = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
                Box<Pull<OperatorMsg<table_slice>>>>{std::move(pull_upstream_)};
    // TODO: Polish this.
    for (auto& op : operators_) {
      auto index = detail::narrow<size_t>(&op - operators_.data());
      match(op, [&]<class In, class Out>(Box<Operator<In, Out>>& op) {
        TENZIR_INFO("got {}", typeid(*op).name());
        auto input = as<Box<Pull<OperatorMsg<In>>>>(std::move(next_input));
        // TODO: This should be parameterized from the outside, right?
        auto [output_sender, output_receiver] = make_op_channel<Out>(1);
        // TODO: This is a horrible hack.
        auto last = index == operators_.size() - 1;
        if (last) {
          if constexpr (std::same_as<Out, Output>) {
            output_sender = std::move(push_downstream_);
          } else {
            TENZIR_UNREACHABLE();
          }
        }
        auto [from_control_sender, from_control_receiver]
          = make_unbounded_channel<FromControl>();
        auto [to_control_sender, to_control_receiver]
          = make_unbounded_channel<ToControl>();
        operator_ctrl_.push_back(std::move(from_control_sender));
        next_input = std::move(output_receiver);
        auto task = run_operator(std::move(op), std::move(input),
                                 std::move(output_sender),
                                 std::move(from_control_receiver),
                                 std::move(to_control_sender), sys_, dh_);
        TENZIR_INFO("spawning operator task");
        queue_.spawn([task = std::move(task),
                      index] mutable -> Task<std::pair<size_t, Shutdown>> {
          co_await std::move(task);
          TENZIR_INFO("got termination from operator {}", index);
          co_return {index, Shutdown{}};
        });
        TENZIR_INFO("inserting control receiver task");
        // FIXME: Need to receive more then once. Async gen?
        queue_.spawn(
          [to_control_receiver = std::move(to_control_receiver), index] mutable
            -> folly::coro::AsyncGenerator<std::pair<size_t, ToControl>> {
            while (true) {
              co_yield {index, co_await to_control_receiver.receive()};
            }
          });
        TENZIR_INFO("done with operator");
      });
    }
  }

  auto run_until_shutdown() -> Task<void> {
    TENZIR_WARN("waiting for all run operators to finish");
    // TODO: Or do we want to continue listening for control responses during
    // shutdown? That would require some additional coordination.
    auto remaining = operators_.size();
    queue_.spawn(from_control_.receive());
    auto got_shutdown = false;
    while (not got_shutdown) {
      TENZIR_WARN("waiting for next info in chain runner");
      auto next = co_await queue_.next();
      // We should never be done here...
      // TODO: Cancellation?
      TENZIR_ASSERT(next, "unexpected end of queue");
      match(
        *next,
        [&](FromControl from_control) {
          match(
            from_control,
            [&](PostCommit) {
              for (auto& ctrl : operator_ctrl_) {
                ctrl.send(PostCommit{});
              }
            },
            [&](Shutdown) {
              TENZIR_INFO("got shutdown notice in subpipeline");
              got_shutdown = true;
            },
            [&](StopOutput) {
              for (auto& ctrl : operator_ctrl_) {
                ctrl.send(Shutdown{});
              }
            });
        },
        [&](std::pair<size_t, variant<Shutdown, ToControl>> next) {
          auto [index, kind] = std::move(next);
          match(
            kind,
            [&](Shutdown) {
              TENZIR_WARN("got shutdown from operator {}", index);
              // Operator terminated. But we didn't send shutdown signal?
              TENZIR_ASSERT(false, "oh no");
            },
            [&](ToControl to_control) {
              TENZIR_WARN("got control message from operator {}: {}", index,
                          to_control);
              switch (to_control) {
                case ToControl::ready_for_shutdown:
                  TENZIR_ASSERT(remaining > 0);
                  remaining -= 1;
                  if (remaining == 0) {
                    // Once we are here, we got a request to shutdown from all
                    // operators. However, since we might be running in a
                    // subpipeline that is not ready to shutdown yet, we first
                    // have to ask control whether we are allowed to.
                    to_control_.send(ToControl::ready_for_shutdown);
                  }
                  break;
                case ToControl::no_more_input:
                  // TODO: Inform the preceding operator that we don't need any
                  // more input.
                  if (index > 0) {
                    operator_ctrl_[index - 1].send(StopOutput{});
                  } else {
                    // TODO: What if we don't host the preceding operator? Then
                    // we need to notify OUR input!
                  }
                  break;
              }
            });
        });
    }
    TENZIR_WARN("sending shutdown to all operators");
    for (auto& sender : operator_ctrl_) {
      sender.send(Shutdown{});
    }
  }

  std::vector<AnyOperator> operators_;
  Box<Pull<OperatorMsg<Input>>> pull_upstream_;
  Box<Push<OperatorMsg<Output>>> push_downstream_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  caf::actor_system& sys_;
  diagnostic_handler& dh_;

  std::vector<Sender<FromControl>> operator_ctrl_;

  QueueScope<variant<
    // Message from our controller.
    FromControl,
    // Message from one of the operators.
    std::pair<
      // Index of the operator where the message came from.
      size_t,
      // Message content.
      variant<
        // Signal that the operator task finished.
        Shutdown,
        // Control message from one of the operators.
        ToControl>>>>
    queue_;
};

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               caf::actor_system& sys, diagnostic_handler& dh) -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await ChainRunner{
    std::move(chain),
    std::move(pull_upstream),
    std::move(push_downstream),
    std::move(from_control),
    std::move(to_control),
    sys,
    dh,
  }
    .run_to_completion();
  TENZIR_INFO("chain runner finished");
}

/// Run a potentially-open pipeline without external control.
template <class Output>
  requires(not std::same_as<Output, void>)
auto run_open_pipeline(OperatorChain<void, Output> pipeline,
                       caf::actor_system& sys, diagnostic_handler& dh)
  -> AsyncGenerator<Output> {
  auto [push_input, pull_input] = make_op_channel<Output>(10);
}

template <class T>
static auto cost(const OperatorMsg<T>& item, size_t limit) -> size_t {
  return std::min(match(
                    item,
                    [](const table_slice& slice) -> size_t {
                      return slice.rows();
                    },
                    [](const chunk_ptr& chunk) -> size_t {
                      return chunk ? chunk->size() : 0;
                    },
                    [](const Signal&) {
                      return size_t{1};
                    }),
                  limit);
};

/// Data channel between two operators.
template <class T>
struct OpChannel {
public:
  explicit OpChannel(size_t limit) : mutex_{Locked{limit}}, limit_{limit} {
    // If we want to allow `limit == 0`, then the logic needs to be adapted to
    // perform a direct transfer if `send` and `receive` are both active.
    TENZIR_ASSERT(limit_ > 0);
  }

  auto send(OperatorMsg<T> x) -> Task<void> {
    TENZIR_VERBOSE("SENDING {:?}", x);
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_ERROR("CANCELLED");
    }};
    auto lock = co_await mutex_.lock();
    TENZIR_VERBOSE("SENDING {:?} MUTEX", x);
    while (true) {
      if (lock->closed) {
        panic("tried to send to closed channel");
      }
      if (cost(x, limit_) <= lock->remaining) {
        break;
      }
      TENZIR_VERBOSE("SPINNING BECAUSE {} > {}", cost(x, limit_),
                     lock->remaining);
      lock.unlock();
      co_await notify_send_.wait();
      lock = co_await mutex_.lock();
    }
    lock->remaining -= cost(x, limit_);
    TENZIR_VERBOSE("SENDING {:?} NOW", x);
    lock->queue.push_back(std::move(x));
    notify_receive_.notify_one();
    guard.disable();
  }

  auto receive() -> Task<OperatorMsg<T>> {
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_DEBUG("CANCELLED");
    }};
    auto lock = co_await mutex_.lock();
    while (lock->queue.empty()) {
      if (lock->closed) {
        panic("tried to receive from empty closed channel");
      }
      lock.unlock();
      co_await notify_receive_.wait();
      lock = co_await mutex_.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    lock->remaining += cost(result, limit_);
    notify_send_.notify_one();
    guard.disable();
    TENZIR_VERBOSE("RECEIVED {:?}", result);
    co_return result;
  }

  /// Close the channel.
  ///
  /// After closing, sending to the channel will fail with a panic. Receiving
  /// from a closed channel will only panic if the channel is empty.
  auto close() -> Task<void> {
    auto lock = co_await mutex_.lock();
    lock->closed = true;
  }

private:
  struct Locked {
    explicit Locked(size_t limit) : remaining{limit} {
    }

    size_t remaining;
    bool closed = false;
    std::deque<OperatorMsg<T>> queue;
  };

  // TODO: This can surely be written better?
  Mutex<Locked> mutex_;
  size_t limit_;
  Notify notify_send_;
  Notify notify_receive_;
};

template <class T>
class OpPush final : public Push<OperatorMsg<T>> {
public:
  explicit OpPush(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  ~OpPush() override {
    if (shared_) {
      // shared_->close();
    }
  }
  OpPush(OpPush&&) = default;
  OpPush& operator=(OpPush&&) = default;
  OpPush(const OpPush&) = delete;
  OpPush& operator=(const OpPush&) = delete;

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    // TENZIR_TODO();
    TENZIR_ASSERT(shared_);
    return shared_->send(std::move(x));
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
class OpPull final : public Pull<OperatorMsg<T>> {
public:
  explicit OpPull(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  ~OpPull() override {
    if (shared_) {
      // shared_->close();
    }
  }
  OpPull(OpPull&&) = default;
  OpPull& operator=(OpPull&&) = default;
  OpPull(const OpPull&) = delete;
  OpPull& operator=(const OpPull&) = delete;

  auto operator()() -> Task<OperatorMsg<T>> override {
    TENZIR_ASSERT(shared_);
    return shared_->receive();
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
auto make_op_channel(size_t limit) -> PushPull<OperatorMsg<T>> {
  auto shared = std::make_shared<OpChannel<T>>(limit);
  return {OpPush<T>{shared}, OpPull<T>{shared}};
}

template auto make_op_channel<void>(size_t limit)
  -> PushPull<OperatorMsg<void>>;

class RunPipelineSettings {
public:
  virtual ~RunPipelineSettings() = default;

  template <class T>
  auto make_operator_channel() -> PushPull<OperatorMsg<T>> {
    if constexpr (std::same_as<T, void>) {
      return make_operator_channel_void();
    } else if constexpr (std::same_as<T, table_slice>) {
      return make_operator_channel_events();
    } else if constexpr (std::same_as<T, chunk_ptr>) {
      return make_operator_channel_bytes();
    } else {
      static_assert(false, "unknown type");
    }
  }

  virtual auto make_operator_channel_void() -> PushPull<OperatorMsg<void>> = 0;

  virtual auto make_operator_channel_events()
    -> PushPull<OperatorMsg<table_slice>>
    = 0;

  virtual auto make_operator_channel_bytes() -> PushPull<OperatorMsg<chunk_ptr>>
    = 0;
};

auto run_pipeline(OperatorChain<void, void> pipeline, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  // FIXME
  auto input = make_op_channel<void>(10).pull;
  auto output = make_op_channel<void>(10).push;
  try {
    auto [from_control_sender, from_control_receiver]
      = make_unbounded_channel<FromControl>();
    auto [to_control_sender, to_control_receiver]
      = make_unbounded_channel<ToControl>();
    auto queue = QueueScope<variant<std::monostate, ToControl>>{};
    co_await queue.activate([&] -> Task<void> {
      queue.spawn([&] -> Task<std::monostate> {
        co_await run_chain(std::move(pipeline), std::move(input),
                           std::move(output), std::move(from_control_receiver),
                           std::move(to_control_sender), sys, dh);
        co_return std::monostate{};
      });
      queue.spawn(to_control_receiver.receive());
      auto is_running = true;
      while (is_running) {
        auto next = co_await queue.next();
        TENZIR_ASSERT(next);
        match(
          *next,
          [&](std::monostate) {
            // TODO: The pipeline terminated?
            TENZIR_INFO("run_pipeline got info that chain terminated");
            is_running = false;
          },
          [&](ToControl to_control) {
            // TODO
            TENZIR_ASSERT(to_control == ToControl::ready_for_shutdown);
            TENZIR_INFO("got shutdown request from outermost subpipeline");
            from_control_sender.send(Shutdown{});
            queue.spawn(to_control_receiver.receive());
          });
      }
      queue.cancel();
    });
  } catch (folly::OperationCancelled) {
    // TODO: ?
    throw;
  } catch (std::exception& e) {
    diagnostic::error("uncaught exception in pipeline: {}", e.what()).emit(dh);
    // TODO: Return failure?
    co_return;
  } catch (...) {
    diagnostic::error("uncaught exception in pipeline").emit(dh);
    // TODO: Return failure?
    co_return;
  }
}

} // namespace tenzir
