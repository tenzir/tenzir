//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include "tenzir/async/unbounded_queue.hpp"

#include <folly/Executor.h>
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

struct PostCommit {};
struct Shutdown {};

using FromControl = variant<PostCommit, Shutdown>;

TENZIR_ENUM(
  /// A message sent from an operator to the controller.
  ToControl,
  /// Notify the host that we are ready to shutdown. After emitting
  /// this, the operator is no longer allowed to send data, so it
  /// should tell its previous operator to stop and its subsequent
  /// operator that it will not get any more input.
  ready_for_shutdown,
  /// Say that we do not want any more input. This will also notify our
  /// preceding operator.
  no_more_input);

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
    TENZIR_VERBOSE("waiting for queue in receiver ({})",
                   fmt::ptr(queue_.get()));
    TENZIR_ASSERT(queue_);
    auto result = co_await queue_->dequeue();
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

/// Convenience wrapper around `folly::coro::AsyncScope`.
class AsyncScope {
public:
  ~AsyncScope() {
    if (needs_join_) {
      TENZIR_ERROR("did not join async scope");
      // This might not work, but we can at least try.
      folly::coro::blockingWait(cancel_and_join());
    }
  }

  AsyncScope() = default;
  AsyncScope(AsyncScope&&) = delete;
  auto operator=(AsyncScope&&) -> AsyncScope& = delete;
  AsyncScope(const AsyncScope&) = delete;
  auto operator=(const AsyncScope&) -> AsyncScope& = delete;

  auto add(Task<void> task) -> Task<void> {
    auto executor = co_await folly::coro::co_current_executor;
    scope_.add(folly::coro::co_withExecutor(executor, std::move(task)));
    needs_join_ = true;
  }

  auto join() -> Task<void> {
    co_await scope_.joinAsync();
    needs_join_ = false;
  }

  auto remaining() -> size_t {
    return scope_.remaining();
  }

  /// This MUST be called before destroying this object!
  auto cancel_and_join() -> Task<void> {
    co_await scope_.cancelAndJoinAsync();
    needs_join_ = false;
  }

private:
  bool needs_join_ = false;
  folly::coro::CancellableAsyncScope scope_;
};

/// This is a bit similar to `folly::coro::merge`, but we can't use that because
/// in our setup, some of the async generators would never finish. This means
/// that the merged generator does not finish. Thus, we have to destroy early,
/// and the docs warn against that:
/// > If the output stream is destroyed early (before reaching end-of-stream or
/// > exception), the remaining input generators are cancelled and detached;
/// > beware of use-after-free.
template <class T>
class QueueScope {
public:
  template <class U>
    requires std::constructible_from<T, U>
  auto add(Task<U> task) -> Task<void> {
    co_await scope_.add(folly::coro::co_invoke(
      [this, task = std::move(task)] mutable -> Task<void> {
        TENZIR_VERBOSE("starting task from queue");
        co_await queue_.enqueue(co_await std::move(task));
        TENZIR_VERBOSE("finished task from queue");
      }));
  }

#if 0
  auto add(Task<void> task) -> Task<void> {
    auto executor = co_await folly::coro::co_current_executor;
    scope_.add(folly::coro::co_withExecutor(executor, std::move(task)));
    needs_join_ = true;
  }
#endif

#if 1
  template <class U>
  auto add(AsyncGenerator<U> gen) -> Task<void> {
    co_await scope_.add(folly::coro::co_invoke(
      [this, gen = std::move(gen)] mutable -> Task<void> {
        TENZIR_VERBOSE("starting async generator from queue");
        while (auto item = co_await gen.next()) {
          TENZIR_VERBOSE("got item in async generator from queue");
          co_await queue_.enqueue(std::move(*item));
        }
      }));
  }
#endif

  auto next() -> Task<T> {
    return queue_.dequeue();
  }

  auto join() -> Task<void> {
    co_await scope_.join();
  }

  auto cancel_and_join() -> Task<void> {
    co_await scope_.cancel_and_join();
  }

private:
  folly::coro::BoundedQueue<T> queue_{1};
  AsyncScope scope_;
};

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
  Runner(Box<Operator<Input, Output>> op, Box<Pull<OperatorMsg<Input>>> input,
         Box<Push<OperatorMsg<Output>>> output,
         Receiver<FromControl> from_control, Sender<ToControl> to_control,
         caf::actor_system& sys)
    : op_{std::move(op)},
      input_{std::move(input)},
      output_{std::move(output)},
      from_control_{std::move(from_control)},
      to_control_{std::move(to_control)},
      sys_{sys} {
  }

  auto run_to_completion() -> Task<void> {
    TENZIR_INFO("entering run loop");
    auto ctx = AsyncCtx{sys_};
    if constexpr (std::same_as<Output, void>) {
      co_await op_->start(ctx);
    } else {
      auto push = OpPushWrapper{output_};
      co_await op_->start(push, ctx);
    }
    co_await queue_.add(op_->await_task());
    co_await queue_.add(input_());
    co_await queue_.add(from_control_.receive());
    while (not got_shutdown_request_) {
      co_await tick();
    }
    TENZIR_WARN("shutting down");
    co_await queue_.cancel_and_join();
    TENZIR_WARN("shutdown done");
  }

private:
  auto tick() -> Task<void> {
    TENZIR_INFO("tick in {}", typeid(*op_).name());
    auto ctx = AsyncCtx{sys_};
    switch (op_->state()) {
      case OperatorState::done:
        co_await handle_done();
        break;
      case OperatorState::unspecified:
        break;
    }
    // FIXME: This might not be the best approach, because we have to cancel
    // futures. We could instead keep them running.
    TENZIR_VERBOSE("waiting in {} for message", typeid(*op_).name());
    auto message = co_await queue_.next();
    co_await match(
      std::move(message),
      [&](std::any message) -> Task<void> {
        // The task provided by the inner implementation completed.
        TENZIR_VERBOSE("got future result in {}", typeid(*op_).name());
        if constexpr (std::same_as<Output, void>) {
          co_await op_->process_task(std::move(message), ctx);
        } else {
          auto push = OpPushWrapper{output_};
          co_await op_->process_task(std::move(message), push, ctx);
        }
        if (op_->state() == OperatorState::done) {
          co_await handle_done();
        } else {
          co_await queue_.add(op_->await_task());
        }
        TENZIR_VERBOSE("handled future result in {}", typeid(*op_).name());
      },
      [&](OperatorMsg<Input> message) -> Task<void> {
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
                co_await op_->process(input, ctx);
              } else {
                auto push = OpPushWrapper{output_};
                co_await op_->process(input, push, ctx);
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
                co_await output_(Signal::checkpoint);
                co_return;
            }
            TENZIR_UNREACHABLE();
          });
        co_await queue_.add(input_());
      },
      [&](FromControl message) -> Task<void> {
        co_await match(
          std::move(message),
          [&](PostCommit) -> Task<void> {
            TENZIR_VERBOSE("got post commit in {}", typeid(*op_).name());
            co_return;
          },
          [&](Shutdown) -> Task<void> {
            // We won't perform any cleanup. This might be undesirable.
            TENZIR_VERBOSE("got shutdown in {}", typeid(*op_).name());
            got_shutdown_request_ = true;
            co_return;
          });
        co_await queue_.add(from_control_.receive());
      });
  }

  auto handle_done() -> Task<void> {
    // We want to run this code once.
    if (is_done_) {
      co_return;
    }
    is_done_ = true;
    auto ctx = AsyncCtx{sys_};
    TENZIR_VERBOSE("...");
    // Immediately inform control that we want no more data.
    if constexpr (not std::same_as<Input, void>) {
      to_control_.send(ToControl::no_more_input);
    }
    // Then finalize the operator, which can still produce output.
    if constexpr (std::same_as<Output, void>) {
      co_await op_->finalize(ctx);
    } else {
      auto push = OpPushWrapper{output_};
      co_await op_->finalize(push, ctx);
      co_await output_(Signal::end_of_data);
    }
    TENZIR_WARN("sending ready to shutdown");
    to_control_.send(ToControl::ready_for_shutdown);
  }

  Box<Operator<Input, Output>> op_;
  Box<Pull<OperatorMsg<Input>>> input_;
  Box<Push<OperatorMsg<Output>>> output_;
  Receiver<FromControl> from_control_;
  Sender<ToControl> to_control_;
  caf::actor_system& sys_;

  QueueScope<variant<std::any, OperatorMsg<Input>, FromControl>> queue_;
  bool got_shutdown_request_ = false;
  bool is_done_ = false;
};

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op,
                  Box<Pull<OperatorMsg<Input>>> input,
                  Box<Push<OperatorMsg<Output>>> output,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, caf::actor_system& sys)
  -> Task<void> {
  co_await Runner<Input, Output>{
    std::move(op),           std::move(input),      std::move(output),
    std::move(from_control), std::move(to_control), sys,
  }
    .run_to_completion();
}

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> input,
               Box<Push<OperatorMsg<Output>>> output, caf::actor_system& sys)
  -> Task<void> {
  auto from_control = std::vector<Sender<FromControl>>{};
  auto operator_scope = AsyncScope{};
  auto receiver_scope = QueueScope<ToControl>{};
  auto operators = std::move(chain).unwrap();
  auto next_input
    = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
              Box<Pull<OperatorMsg<table_slice>>>>{std::move(input)};
  // TODO: Polish this.
  for (auto& op : operators) {
    auto last = &op == &operators.back();
    co_await match(
      op, [&]<class In, class Out>(Box<Operator<In, Out>>& op) -> Task<void> {
        auto input = as<Box<Pull<OperatorMsg<In>>>>(std::move(next_input));
        auto [output_sender, output_receiver] = make_op_channel<Out>(5);
        // TODO: This is a horrible hack.
        if (last) {
          if constexpr (std::same_as<Out, Output>) {
            output_sender = std::move(output);
          } else {
            TENZIR_UNREACHABLE();
          }
        }
        auto [from_control_sender, from_control_receiver]
          = make_unbounded_channel<FromControl>();
        auto [to_control_sender, to_control_receiver]
          = make_unbounded_channel<ToControl>();
        from_control.push_back(std::move(from_control_sender));
        next_input = std::move(output_receiver);
        auto task = run_operator(std::move(op), std::move(input),
                                 std::move(output_sender),
                                 std::move(from_control_receiver),
                                 std::move(to_control_sender), sys);
        co_await operator_scope.add(std::move(task));
        co_await receiver_scope.add(
          std::move(to_control_receiver).into_generator());
      });
  }
  TENZIR_WARN("waiting for all run tasks to finish");
  // TODO: Or do we want to continue listening for control responses during
  // shutdown? That would require some additional coordination.
  auto remaining = operators.size();
  while (remaining > 0) {
    auto next = co_await receiver_scope.next();
    TENZIR_WARN("got next: {}", next);
    switch (next) {
      case ToControl::ready_for_shutdown:
        remaining -= 1;
        break;
      case ToControl::no_more_input:
        break;
    }
  }
  TENZIR_WARN("got all shutdown");
  for (auto& sender : from_control) {
    sender.send(Shutdown{});
  }
  co_await receiver_scope.cancel_and_join();
  TENZIR_WARN("joined receivers");
  co_await operator_scope.join();
  TENZIR_WARN("joined operators");
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
  }

  auto send(OperatorMsg<T> x) -> Task<void> {
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_ERROR("CANCELLED");
    }};
    auto lock = co_await mutex_.lock();
    while (cost(x, limit_) > lock->remaining) {
      lock.unlock();
      co_await remaining_increased_.wait();
      lock = co_await mutex_.lock();
    }
    lock->remaining -= cost(x, limit_);
    lock->queue.push_back(x);
    queue_pushed_.notify_one();
    guard.disable();
  }

  auto receive() -> Task<OperatorMsg<T>> {
    auto guard = detail::scope_guard{[] noexcept {
      TENZIR_DEBUG("CANCELLED");
    }};
    auto lock = co_await mutex_.lock();
    while (lock->queue.empty()) {
      lock.unlock();
      co_await queue_pushed_.wait();
      lock = co_await mutex_.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    lock->remaining += cost(result, limit_);
    remaining_increased_.notify_one();
    guard.disable();
    co_return result;
  }

private:
  struct Locked {
    explicit Locked(size_t limit) : remaining{limit} {
    }

    size_t remaining;
    std::deque<OperatorMsg<T>> queue;
  };

  // TODO: This can surely be written better?
  size_t limit_;
  Mutex<Locked> mutex_;
  Notify remaining_increased_;
  Notify queue_pushed_;
};

template <class T>
class OpPush final : public Push<OperatorMsg<T>> {
public:
  explicit OpPush(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
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

  auto operator()() -> Task<OperatorMsg<T>> override {
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

auto run_pipeline(OperatorChain<void, void> pipeline,
                  Box<Pull<OperatorMsg<void>>> input,
                  Box<Push<OperatorMsg<void>>> output, caf::actor_system& sys)
  -> Task<void> {
  return run_chain(std::move(pipeline), std::move(input), std::move(output),
                   sys);
}

} // namespace tenzir
