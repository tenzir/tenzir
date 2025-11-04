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
class BadAsyncScope {
public:
  ~BadAsyncScope() {
    if (needs_join_) {
      TENZIR_ERROR("did not join async scope");
      // This might not work, but we can at least try.
      folly::coro::blockingWait(cancel_and_join());
    }
  }

  BadAsyncScope() = default;
  BadAsyncScope(BadAsyncScope&&) = delete;
  auto operator=(BadAsyncScope&&) -> BadAsyncScope& = delete;
  BadAsyncScope(const BadAsyncScope&) = delete;
  auto operator=(const BadAsyncScope&) -> BadAsyncScope& = delete;

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

/// The reified result of an asynchronous computation.
///
/// Either it produced a value, or it failed, or it was cancelled.
template <class T>
class AsyncResult {
public:
  AsyncResult() = default;
  ~AsyncResult() = default;

  // explicit(false) FollyResult(folly::result<T> value) {
  //   if (value_.hasValue()) {
  //     value_ = std::move(value).value();
  //   }
  // }

  AsyncResult(AsyncResult<T>&& other) = default;
  auto operator=(AsyncResult<T>&&) -> AsyncResult<T>& = default;
  AsyncResult(const AsyncResult<T>& other) = default;
  auto operator=(const AsyncResult<T>&) -> AsyncResult<T>& = default;

  template <class U>
    requires std::convertible_to<U, T>
  explicit(false) AsyncResult(AsyncResult<U> other)
    : value_{other.is_value() ? folly::Try<T>{std::move(other).value()}
                              : folly::Try<T>{std::move(other).exception()}} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) AsyncResult(U&& value) : value_{std::forward<U>(value)} {
  }

  explicit(false) AsyncResult(folly::exception_wrapper ew)
    : value_{std::move(ew)} {
  }

  explicit(false) AsyncResult(folly::Try<T> value) : value_{std::move(value)} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) AsyncResult(folly::Try<U> value) {
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
class LegacyQueueScope {
public:
  using Next = std::conditional_t<std::same_as<T, void>, std::monostate, T>;

  ~LegacyQueueScope() {
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
    // FIXME: Make it like above?
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
            = AsyncResult{co_await folly::coro::co_awaitTry(gen.next())};
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
  /// Rethrows inner exceptions. Cancellations of the inner task are discarded.
  /// Returns `nullopt` if all submitted tasks have finished.
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
      // If we are cancelled, then we still need to wait for the inner tasks.
      auto result = AsyncResult{co_await folly::coro::co_awaitTry(next())};
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
  folly::coro::BoundedQueue<std::optional<AsyncResult<T>>> queue_{999};
  folly::coro::CancellableAsyncScope scope_;
};

template <class T>
struct AsyncHandleState {
  bool notified = false;
  Notify notify;
  AsyncResult<T> value;
};

/// Handle to an asynchronous, scoped task that was spawned.
template <class T>
class AsyncHandle {
public:
  /// Wait for the associated task to complete and return its result.
  ///
  /// If a call to this function is cancelled, then the underlying task will not
  /// be joined. May be called at most once.
  auto join() -> Task<AsyncResult<T>> {
    TENZIR_ASSERT(state_);
    TENZIR_ASSERT(not state_->notified);
    co_await state_->notify.wait();
    state_->notified = true;
    co_return std::move(state_->value).unwrap();
  }

private:
  friend class AsyncScope;

  explicit AsyncHandle(std::shared_ptr<AsyncHandleState<T>> state)
    : state_{std::move(state)} {
  }

  std::shared_ptr<AsyncHandleState<T>> state_;
};

/// Utility type created by `async_scope`.
class AsyncScope {
public:
  /// Spawn an awaitable, for example a task.
  ///
  /// The returned handle can be used to join the awaitable and returns its
  /// result. When dropped without joining, the awaitable continues running.
  template <class SemiAwaitable>
  auto spawn(SemiAwaitable&& awaitable)
    -> AsyncHandle<folly::coro::semi_await_result_t<SemiAwaitable>> {
    using Result = folly::coro::semi_await_result_t<SemiAwaitable>;
    auto state = std::make_shared<AsyncHandleState<Result>>();
    scope_.add(
      folly::coro::co_withExecutor(
        executor_,
        folly::coro::co_invoke([state, awaitable = std::forward<SemiAwaitable>(
                                         awaitable)] mutable -> Task<void> {
          state->value
            = co_await folly::coro::co_awaitTry(std::move(awaitable));
          state->notify.notify_one();
        })),
      std::nullopt, FOLLY_ASYNC_STACK_RETURN_ADDRESS());
    return AsyncHandle{std::move(state)};
  }

  /// Spawn a function.
  template <class F>
  auto spawn(F&& f)
    -> AsyncHandle<folly::coro::semi_await_result_t<std::invoke_result_t<F>>> {
    // We need to `co_invoke` to ensure that the function itself is kept alive.
    return (*this)(folly::coro::co_invoke(std::forward<F>(f)));
  }

  /// Cancel all remaining tasks.
  auto cancel() {
    // TODO: Exact behavior?
    scope_.requestCancellation();
  }

private:
  AsyncScope(const AsyncScope&) = delete;
  auto operator=(const AsyncScope&) -> AsyncScope& = delete;
  AsyncScope(AsyncScope&&) = delete;
  auto operator=(AsyncScope&&) -> AsyncScope& = delete;

  AsyncScope(folly::ExecutorKeepAlive<> executor,
             folly::coro::CancellableAsyncScope& scope)
    : executor_{std::move(executor)}, scope_{scope} {
  }
  ~AsyncScope() = default;

  folly::ExecutorKeepAlive<> executor_;
  folly::coro::CancellableAsyncScope& scope_;

  template <class F>
    requires std::invocable<F, AsyncScope&>
  friend auto async_scope(F&& f) -> Task<
    folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>>;
};

/// Provides a scope that can spawn tasks for structured concurrency.
///
/// The given function and all tasks spawned may access external objects as long
/// as they outlive this function call. It will only return once all spawned
/// tasks have been completed. If the function fails or is cancelled, then all
/// spawned tasks will be cancelled.
///
/// Fun fact: This function is one of the very few things that would not be
/// possible in Rust, since implementing it requires async cancellation.
template <class F>
  requires std::invocable<F, AsyncScope&>
auto async_scope(F&& f) -> Task<
  folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>> {
  auto scope = folly::coro::CancellableAsyncScope{folly::CancellationToken{
    co_await folly::coro::co_current_cancellation_token}};
  auto spawn = AsyncScope{co_await folly::coro::co_current_executor, scope};
  // For memory safety, after calling the user-provided function, we must under
  // all circumstances reach the cleanup and join all spawned tasks, since they
  // may reference objects that might be destroyed once this function returns.
  auto result = AsyncResult{
    co_await folly::coro::co_awaitTry(std::invoke(std::forward<F>(f), spawn))};
  // We only cancel the jobs if the given function failed or was cancelled.
  if (not result.is_value()) {
    scope.requestCancellation();
  }
  // Provide a custom cancellation token to ensure that cancellation doesn't
  // abort the join.
  auto joined = AsyncResult{co_await folly::coro::co_awaitTry(
    folly::coro::co_withCancellation({}, scope.joinAsync()))};
  // If the join fails, then we cannot continue as that might compromise memory
  // safety due to lifetime issues. However, because we prevent cancellation and
  // do not ask the scope itself to store and rethrow exceptions, this should
  // never happen. But just in case, we check and abort.
  if (not joined.is_value()) {
    TENZIR_ERROR("aborting because async scope join failed");
    std::terminate();
  }
  /// Now return the result of the user-provided function.
  co_return std::move(result).unwrap();
}

// TODO: Backpressure?
template <class T>
class QueueScope {
public:
  template <class U>
  auto activate(Task<U> task) -> Task<U> {
    co_return co_await async_scope([&](AsyncScope& scope) -> Task<U> {
      // We sneakily store the reference to the spawner here, making sure that
      // we reset it once we leave the async scope, as required for correctness.
      TENZIR_ASSERT(not scope_);
      scope_ = &scope;
      auto guard = detail::scope_guard{[&] noexcept {
        scope_ = nullptr;
      }};
      co_return co_await std::move(task);
      // TODO: What about the queue at this point?
    });
  }

  template <class F>
  auto activate(F&& f) -> Task<void> {
    // TODO: Fix typing.
    return activate(folly::coro::co_invoke(std::forward<F>(f)));
  }

  // TODO: Signature?
  // TODO: Thread-safety?
  template <class U>
  void spawn(Task<U> task) {
    TENZIR_ASSERT(scope_);
    scope_->spawn(folly::coro::co_invoke(
      [this, task = std::move(task)] mutable -> Task<void> {
        queue_.enqueue(co_await folly::coro::co_awaitTry(std::move(task)));
      }));
    remaining_ += 1;
  }

  template <class F>
    requires std::invocable<F>
  void spawn(F&& f) {
    TENZIR_ASSERT(scope_);
    scope_->spawn(folly::coro::co_invoke(
      [this, f = std::forward<F>(f)] mutable -> Task<void> {
        queue_.enqueue(
          co_await folly::coro::co_awaitTry(std::invoke(std::move(f))));
      }));
    remaining_ += 1;
  }

  /// Cancel all remaining tasks.
  void cancel() {
    // TODO: Exact behavior.
    TENZIR_ASSERT(scope_);
    scope_->cancel();
  }

  using Next = std::conditional_t<std::is_same_v<T, void>, std::monostate, T>;

  /// Retrieve the next task result or return `nullopt` if none remain.
  ///
  /// This function can be called while the scope is active, but also when it
  /// already got deactivated. If a task failed, we rethrow theexception.
  /// TODO: What if it got cancelled?
  auto next() -> Task<std::optional<Next>> {
    if (remaining_ == 0) {
      co_return {};
    }
    auto result = co_await queue_.dequeue();
    remaining_ -= 1;
    if constexpr (std::same_as<T, void>) {
      std::move(result).unwrap();
      co_return std::monostate{};
    } else {
      co_return std::move(result).unwrap();
    }
  }

private:
  std::atomic<size_t> remaining_ = 0;
  folly::coro::UnboundedQueue<AsyncResult<T>> queue_;
  AsyncScope* scope_ = nullptr;
};

template <class T>
class AsyncScope2 {
public:
  /// Activate the scope for the duration of the inner task.
  ///
  /// You can only add tasks to scopes that are active. This function will only
  /// return once all submitted tasks have finished. Thus, your tasks may access
  /// external objects that outlive the function call (but not any others).
  template <class U>
  auto activate(Task<U> task) -> Task<U> {
    TENZIR_ASSERT(not active_);
    active_ = true;
    auto result
      = AsyncResult{co_await folly::coro::co_awaitTry(std::move(task))};
    // We only cancel the jobs if the task failed or was cancelled.
    if (not result.is_value()) {
      queue_.cancel();
    }
    // TODO: Can this fail or be cancelled?
    // For memory safety, we need to ensure that the joining is not cancelled.
    // Hence, we construct a new cancellation token. We also use `co_awaitTry`
    // because `.join()` rethrows exceptions after it's done.
    auto join = AsyncResult{co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation({}, queue_.join()))};
    active_ = false;
    // Prefer the error from the actual task because that came earlier.
    if (not result.is_value()) {
      co_yield folly::coro::co_error(
        std::move(result).exception_or_cancelled());
    }
    if (not join.is_value()) {
      // TODO: We assume that the actual joining succeeded but a submitted task
      // failed. The cancellation of inner tasks is ignored so we know that it
      // must be an exception.
      co_yield folly::coro::co_error(std::move(join).exception());
    }
    co_return std::move(result).unwrap();
  }

  template <class U>
  auto add(Task<U> task) -> Task<void> {
    TENZIR_ASSERT(active_);
    return queue_.add(std::move(task));
  }

  template <class U>
  auto add(AsyncGenerator<U> task) -> Task<void> {
    TENZIR_ASSERT(active_);
    return queue_.add(std::move(task));
  }

  auto next() -> Task<std::optional<T>> {
    TENZIR_ASSERT(active_);
    return queue_.next();
  }

private:
  bool active_ = false;
  LegacyQueueScope<T> queue_;
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

  auto run_to_completion() && -> Task<void> {
    // Immediately check for cancellation and allow rescheduling.
    return queue_.activate(run());
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
        co_return;
      },
      [&](Shutdown) -> Task<void> {
        // We won't perform any cleanup. This might be undesirable.
        TENZIR_VERBOSE("got shutdown in {}", typeid(*op_).name());
        got_shutdown_request_ = true;
        co_return;
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

template <class Input, class Output>
class ChainRunner {
public:
  ChainRunner(OperatorChain<Input, Output> chain,
              Box<Pull<OperatorMsg<Input>>> input,
              Box<Push<OperatorMsg<Output>>> output, caf::actor_system& sys,
              diagnostic_handler& dh)
    : chain_{std::move(chain)},
      input_{std::move(input)},
      output_{std::move(output)},
      sys_{sys},
      dh_{dh} {
  }

  auto run_to_completion() && -> Task<void> {
    return operator_queue_.activate([&] {
      return receiver_queue_.activate([&] {
        return run();
      });
    });
  }

private:
  auto run() -> Task<void> {
    TENZIR_WARN("beginning chain setup");
    auto from_control = std::vector<Sender<FromControl>>{};
    auto operators = std::move(chain_).unwrap();
    auto next_input
      = variant<Box<Pull<OperatorMsg<void>>>, Box<Pull<OperatorMsg<chunk_ptr>>>,
                Box<Pull<OperatorMsg<table_slice>>>>{std::move(input_)};
    // TODO: Polish this.
    for (auto& op : operators) {
      auto last = &op == &operators.back();
      match(op, [&]<class In, class Out>(Box<Operator<In, Out>>& op) {
        TENZIR_INFO("got {}", typeid(*op).name());
        auto input = as<Box<Pull<OperatorMsg<In>>>>(std::move(next_input));
        auto [output_sender, output_receiver] = make_op_channel<Out>(999);
        // TODO: This is a horrible hack.
        if (last) {
          if constexpr (std::same_as<Out, Output>) {
            output_sender = std::move(output_);
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
                                 std::move(to_control_sender), sys_, dh_);
        TENZIR_INFO("spawning operator task");
        operator_queue_.spawn(std::move(task));
        TENZIR_INFO("inserting control receiver task");
        // FIXME: Need to receive more then once. Async gen?
        receiver_queue_.spawn(
          [this, to_control_receiver = std::move(to_control_receiver)](
            this auto&& self) -> Task<ToControl> {
            auto result = co_await to_control_receiver.receive();
            // TODO: This might be unsafe, and also just a bad idea.
            receiver_queue_.spawn(folly::coro::co_invoke(std::move(self)));
            co_return result;
          });
        TENZIR_INFO("done with operator");
      });
    }
    auto test_immediate_cancellation = false;
    if (test_immediate_cancellation) {
      operator_queue_.cancel();
    }
    TENZIR_WARN("waiting for all run operators to finish");
    // TODO: Or do we want to continue listening for control responses during
    // shutdown? That would require some additional coordination.
    auto remaining = operators.size();
    // TODO: Refactor?
    auto combined_queue = QueueScope<std::optional<ToControl>>{};
    co_await combined_queue.activate([&] -> Task<void> {
      combined_queue.spawn(receiver_queue_.next());
      combined_queue.spawn(folly::coro::co_invoke([&] -> Task<std::nullopt_t> {
        co_await operator_queue_.next();
        co_return std::nullopt;
      }));
      while (remaining > 0) {
        TENZIR_WARN("waiting for next info in chain runner");
        auto next = co_await combined_queue.next();
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
          combined_queue.spawn(receiver_queue_.next());
        } else {
          // Operator terminated. But we didn't send shutdown signal?
          TENZIR_ASSERT(false, "oh no");
        }
      }
      TENZIR_WARN("sending shutdown to all operators");
      for (auto& sender : from_control) {
        sender.send(Shutdown{});
      }
      TENZIR_WARN("cancelling combined and receiver");
      combined_queue.cancel();
      receiver_queue_.cancel();
    });
  }

  OperatorChain<Input, Output> chain_;
  Box<Pull<OperatorMsg<Input>>> input_;
  Box<Push<OperatorMsg<Output>>> output_;
  caf::actor_system& sys_;
  diagnostic_handler& dh_;

  QueueScope<void> operator_queue_;
  QueueScope<ToControl> receiver_queue_;
};

template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> input,
               Box<Push<OperatorMsg<Output>>> output, caf::actor_system& sys,
               diagnostic_handler& dh) -> Task<void> {
  co_await folly::coro::co_safe_point;
  co_await ChainRunner{
    std::move(chain), std::move(input), std::move(output), sys, dh,
  }
    .run_to_completion();
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

template auto make_op_channel<void>(size_t limit)
  -> PushPull<OperatorMsg<void>>;

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
