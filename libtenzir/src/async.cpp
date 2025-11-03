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

  // FIXME: This is not safe if the task fails or is cancelled!!
  auto add(Task<void> task) -> Task<void> {
    // auto executor = co_await folly::coro::co_current_executor;
    // scope_.add(folly::coro::co_withExecutor(executor, std::move(task)));
    co_await scope_.co_schedule(std::move(task));
    needs_join_ = true;
  }

  auto join() -> Task<void> {
    co_await scope_.joinAsync();
    needs_join_ = false;
  }

  auto pending() const -> size_t {
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

/// A convenience wrapper for `folly::result<T>` and `folly::Try<T>`.
template <class T>
class FollyResult {
public:
  FollyResult() = default;
  ~FollyResult() = default;

  // explicit(false) FollyResult(folly::result<T> value) {
  //   if (value_.hasValue()) {
  //     value_ = std::move(value).value();
  //   }
  // }

  FollyResult(FollyResult<T>&& other) = default;
  auto operator=(FollyResult<T>&&) -> FollyResult<T>& = default;
  FollyResult(const FollyResult<T>& other) = default;
  auto operator=(const FollyResult<T>&) -> FollyResult<T>& = default;

  template <class U>
    requires std::convertible_to<U, T>
  explicit(false) FollyResult(FollyResult<U> other)
    : value_{other.is_value() ? folly::Try<T>{std::move(other).value()}
                              : folly::Try<T>{std::move(other).exception()}} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) FollyResult(U&& value) : value_{std::forward<U>(value)} {
  }

  explicit(false) FollyResult(folly::exception_wrapper ew)
    : value_{std::move(ew)} {
  }

  explicit(false) FollyResult(folly::Try<T> value) : value_{std::move(value)} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) FollyResult(folly::Try<U> value) {
    if (value.hasValue()) {
      value_.emplace(std::move(value).value());
    } else {
      value_.emplaceException(std::move(value).exception());
    }
  }

  template <class Self>
  auto unwrap(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).value_.value();
  }

  template <class Self>
  auto exception(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(not self.is_cancelled());
    return std::forward<Self>(self).exception_or_cancelled();
  }

  template <class Self>
  auto exception_or_cancelled(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).value_.exception();
  }

  auto is_cancelled() const -> bool {
    return value_.template hasException<folly::OperationCancelled>();
  }

  auto is_value() const -> bool {
    return value_.hasValue();
  }

  auto is_exception() const -> bool {
    return value_.hasException() and not is_cancelled();
  }

private:
  // Based on `Try` because `result` is not default-constructible.
  folly::Try<T> value_;
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
  using Next = std::conditional_t<std::same_as<T, void>, std::monostate, T>;

  ~QueueScope() {
    if (needs_join_) {
      TENZIR_ERROR("did not join {}", fmt::ptr(this));
    }
  }

  template <class U>
    requires(std::constructible_from<T, U>
             or (std::same_as<T, void> and std::same_as<U, void>))
  auto add(Task<U> task) -> Task<void> {
    TENZIR_WARN("adding task to {} ({})", fmt::ptr(this), typeid(T).name());
    pending_ += 1;
    needs_join_ = true;
    scope_.add(
      folly::coro::co_withExecutor(
        co_await folly::coro::co_current_executor,
        folly::coro::co_invoke([this, task
                                      = std::move(task)] mutable -> Task<void> {
          // TODO: Do we know that we reach this if we get cancelled?
          co_await folly::coro::co_scope_exit(
            [](auto* self) -> Task<void> {
              // TODO: But what if the reading end was cancelled as well??
              co_await self->queue_.enqueue(std::nullopt);
              TENZIR_VERBOSE("enqueued nullopt for task from queue: {}",
                             fmt::ptr(self));
            },
            this);
          TENZIR_VERBOSE("starting task from queue {}", fmt::ptr(this));
          auto result = co_await folly::coro::co_awaitTry(std::move(task));
          TENZIR_VERBOSE("attempting enqueue for queue {}", fmt::ptr(this));
          // WHAT IF WE GET CANCELLED HERE?
          co_await queue_.enqueue(std::move(result));
          TENZIR_VERBOSE("did enqueue result of task from queue: {}",
                         fmt::ptr(this));
        })),
      // TODO: Inject this once in an async constructor.
      co_await folly::coro::co_current_cancellation_token,
      FOLLY_ASYNC_STACK_RETURN_ADDRESS());
  }

#if 0
  auto add(Task<void> task) -> Task<void> {
    auto executor = co_await folly::coro::co_current_executor;
    scope_.add(folly::coro::co_withExecutor(executor, std::move(task)));
    needs_join_ = true;
  }
#endif

#if 1
  // TODO: How should this handle cancellation?
  template <class U>
  auto add(AsyncGenerator<U> gen) -> Task<void> {
    TENZIR_WARN("adding task to {}", fmt::ptr(this));
    pending_ += 1;
    needs_join_ = true;
    co_await scope_.co_schedule(
      folly::coro::co_invoke([this, gen
                                    = std::move(gen)] mutable -> Task<void> {
        TENZIR_VERBOSE("starting async generator from queue: {}",
                       fmt::ptr(this));
        // TODO: Do we know that we reach this if we get cancelled?
        co_await folly::coro::co_scope_exit(
          [](auto* self) -> Task<void> {
            // TODO: But what if the reading end was cancelled as well??
            co_await self->queue_.enqueue(std::nullopt);
            TENZIR_VERBOSE("enqueued nullopt for generator from queue: {}",
                           fmt::ptr(self));
          },
          this);
        while (true) {
          auto result
            = FollyResult{co_await folly::coro::co_awaitTry(gen.next())};
          if (result.is_value()) {
            auto next = std::move(result).unwrap();
            if (not next.has_value()) {
              // The generator is exhausted.
              break;
            }
            TENZIR_VERBOSE("got item in async generator from queue");
            co_await queue_.enqueue(std::move(next).value());
          } else {
            TENZIR_VERBOSE("got exception in async generator from queue");
            co_await queue_.enqueue(std::move(result).exception_or_cancelled());
            break;
          }
        }
        TENZIR_VERBOSE("ended async generator from queue");
      }),
      co_await folly::coro::co_current_cancellation_token);
  }
#endif

  /// Dequeue the next item from the queue.
  ///
  /// Rethrows inner exceptions. Cancellations are discarded.
  auto next() -> Task<std::optional<Next>> {
    while (pending_ > 0) {
      TENZIR_WARN("about to dequeue from {} (cancel = {})", fmt::ptr(this),
                  (co_await folly::coro::co_current_cancellation_token)
                    .isCancellationRequested());
      auto result = co_await queue_.dequeue();
      TENZIR_WARN("dequeue from {} done", fmt::ptr(this));
      if (not result) {
        // Used as a signal that a task terminated.
        pending_ -= 1;
        TENZIR_INFO("completed task from queue, {} remaining", pending_);
        if (pending_ == 0) {
          TENZIR_ASSERT(scope_.remaining() == 0);
          co_await scope_.joinAsync();
          needs_join_ = false;
          TENZIR_INFO("joined async scope: {}", fmt::ptr(this));
          // We leave a new scope behind so that we could start again.
          // TODO: This is very ugly.
          scope_.~CancellableAsyncScope();
          new (&scope_) folly::coro::CancellableAsyncScope{};
        }
        continue;
      }
      if (result->is_cancelled()) {
        // TODO: We ignore the result.
        TENZIR_INFO("got cancel from task in queue");
        continue;
      }
      TENZIR_INFO("got result from task in queue");
      if constexpr (std::same_as<T, void>) {
        result->unwrap();
        co_return std::monostate{};
      } else {
        co_return std::move(result)->unwrap();
      }
    }
    co_return std::nullopt;
  }

  /// Cancel all tasks running under this scope.
  void cancel() {
    TENZIR_INFO("cancelling scope {}", fmt::ptr(this));
    scope_.requestCancellation();
  }

  auto cancel_and_join() -> Task<void> {
    cancel();
    co_await join();
  }

  auto join() -> Task<void> {
    auto exception = std::optional<folly::exception_wrapper>{};
    while (true) {
      auto result = FollyResult{co_await folly::coro::co_awaitTry(next())};
      if (result.is_value()) {
        auto next = std::move(result).unwrap();
        if (next.has_value()) {
          // Ignore values.
        } else {
          // We are done.
          break;
        }
      } else if (result.is_exception()) {
        // Remember only the first exception.
        if (not exception) {
          exception = std::move(result.exception());
        }
      } else {
        // Ignore cancellations.
      }
    }
    if (exception) {
      co_yield folly::coro::co_error(std::move(*exception));
    }
  }

  // auto join() -> Task<void> {
  //   co_await scope_.join();
  // }

  // auto cancel_and_join() -> Task<void> {
  //   co_await scope_.cancelAndJoinAsync();
  // }

  auto pending() const -> size_t {
    return pending_;
  }

  auto has_pending() const -> bool {
    // TODO: Does this work?
    return pending_ > 0;
  }

private:
  size_t pending_ = 0;
  // TODO: Bigger/unbounded queue?
  bool needs_join_ = false;
  folly::coro::BoundedQueue<std::optional<FollyResult<T>>> queue_{999};
  folly::coro::CancellableAsyncScope scope_;
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

  auto run_to_completion() -> Task<void> {
    TENZIR_INFO("entering run loop of {}", typeid(*op_).name());
    co_await folly::coro::co_scope_exit(
      [](Runner* self) -> Task<void> {
        TENZIR_WARN("shutting down operator {} with {} pending",
                    typeid(*self->op_).name(), self->queue_.pending());
        // TODO: Can we always do this here?
        co_await self->queue_.cancel_and_join();
        TENZIR_WARN("shutdown done for {}", typeid(*self->op_).name());
      },
      this);
    try {
      TENZIR_INFO("-> pre start");
      if constexpr (std::same_as<Output, void>) {
        co_await op_->start(ctx_);
      } else {
        auto push = OpPushWrapper{push_downstream_};
        co_await op_->start(push, ctx_);
      }
      TENZIR_INFO("-> post start");
      co_await queue_.add(op_->await_task());
      co_await queue_.add(pull_upstream_());
      co_await queue_.add(from_control_.receive());
      while (not got_shutdown_request_) {
        co_await folly::coro::co_safe_point;
        co_await tick();
      }
    } catch (folly::OperationCancelled) {
      TENZIR_VERBOSE("shutting down operator after cancellation");
    } catch (std::exception& e) {
      TENZIR_ERROR("shutting down operator after uncaught exception: {}",
                   e.what());
      throw;
    } catch (...) {
      TENZIR_ERROR("shutting down operator after uncaught exception");
      throw;
    }
  }

private:
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
    // FIXME: This might not be the best approach, because we have to cancel
    // futures. We could instead keep them running.
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
      co_await queue_.add(op_->await_task());
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
    co_await queue_.add(pull_upstream_());
  }

  auto process(FromControl message) -> Task<void> {
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

template <class Input, class Output>
auto run_operator(Box<Operator<Input, Output>> op,
                  Box<Pull<OperatorMsg<Input>>> pull_upstream,
                  Box<Push<OperatorMsg<Output>>> push_downstream,
                  Receiver<FromControl> from_control,
                  Sender<ToControl> to_control, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  // Immediately check for cancellation and allow rescheduling.
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

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> input,
               Box<Push<OperatorMsg<Output>>> output, caf::actor_system& sys,
               diagnostic_handler& dh) -> Task<void> {
  TENZIR_WARN("beginning chain setup");
  auto from_control = std::vector<Sender<FromControl>>{};
  auto&& [operator_scope] = co_await folly::coro::co_scope_exit(
    [](Box<QueueScope<void>> queue) -> Task<void> {
      co_await queue->cancel_and_join();
    },
    Box<QueueScope<void>>{std::in_place});
  auto&& [receiver_scope] = co_await folly::coro::co_scope_exit(
    [](Box<QueueScope<ToControl>> queue) -> Task<void> {
      co_await queue->cancel_and_join();
    },
    Box<QueueScope<ToControl>>{std::in_place});
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
        auto [output_sender, output_receiver] = make_op_channel<Out>(999);
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
                                 std::move(to_control_sender), sys, dh);
        co_await operator_scope->add(std::move(task));
        co_await receiver_scope->add(
          std::move(to_control_receiver).into_generator());
      });
  }
  auto test_immediate_cancellation = false;
  if (test_immediate_cancellation) {
    operator_scope->cancel();
  }
  TENZIR_WARN("waiting for all run tasks to finish");
  // TODO: Or do we want to continue listening for control responses during
  // shutdown? That would require some additional coordination.
  auto remaining = operators.size();
  auto&& [combined_scope] = co_await folly::coro::co_scope_exit(
    [](Box<QueueScope<std::optional<ToControl>>> queue) -> Task<void> {
      co_await queue->cancel_and_join();
    },
    Box<QueueScope<std::optional<ToControl>>>{std::in_place});
  co_await combined_scope->add(receiver_scope->next());
  co_await combined_scope->add(
    folly::coro::co_invoke([&] -> Task<std::nullopt_t> {
      co_await operator_scope->next();
      co_return std::nullopt;
    }));
  while (remaining > 0) {
    TENZIR_WARN("waiting for next info in chain runner");
    auto next = co_await combined_scope->next();
    // We should never be done here...
    // TODO: Cancellation?
    TENZIR_ASSERT(next);
    TENZIR_WARN("got next: {}", *next);
    if (next->has_value()) {
      // Control message.
      switch (next->value()) {
        case ToControl::ready_for_shutdown:
          remaining -= 1;
          break;
        case ToControl::no_more_input:
          break;
      }
      co_await combined_scope->add(receiver_scope->next());
    } else {
      // Operator terminated. But we didn't send shutdown signal?
      TENZIR_ASSERT(false, "oh no");
    }
  }
  TENZIR_WARN("sending shutdown to all operators");
  for (auto& sender : from_control) {
    sender.send(Shutdown{});
  }
  TENZIR_WARN("joining combined scope");
  co_await combined_scope->cancel_and_join();
  TENZIR_WARN("joining receivers");
  co_await receiver_scope->cancel_and_join();
  TENZIR_WARN("joining operators");
  co_await operator_scope->join();
  TENZIR_WARN("exiting run");
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
  size_t limit_;
  Mutex<Locked> mutex_;
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

auto run_pipeline(OperatorChain<void, void> pipeline,
                  Box<Pull<OperatorMsg<void>>> input,
                  Box<Push<OperatorMsg<void>>> output, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void> {
  try {
    co_await run_chain(std::move(pipeline), std::move(input), std::move(output),
                       sys, dh);
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
