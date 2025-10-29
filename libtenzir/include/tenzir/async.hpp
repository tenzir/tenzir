//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

#include <folly/coro/Baton.h>
#include <folly/coro/Collect.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Synchronized.h>
#include <folly/coro/Task.h>
#include <folly/coro/Traits.h>

#include <any>

namespace tenzir {

/// A non-null `std::unique_ptr`.
template <class T>
class Box {
public:
  static auto from_non_null(std::unique_ptr<T> ptr) -> Box<T> {
    return Box{std::move(ptr)};
  }

  template <class U>
    requires std::convertible_to<std::unique_ptr<U>, std::unique_ptr<T>>
  explicit(false) Box(U x) : ptr_{std::make_unique<U>(std::move(x))} {
  }

  template <class... Ts>
  explicit(false) Box(std::in_place_t, Ts&&... xs)
    : ptr_{std::make_unique<T>(std::forward<Ts>(xs)...)} {
  }

  auto operator->() -> T* {
    TENZIR_ASSERT(ptr_);
    return ptr_.get();
  }

  auto operator->() const -> T const* {
    TENZIR_ASSERT(ptr_);
    return ptr_.get();
  }

  auto operator*() -> T& {
    TENZIR_ASSERT(ptr_);
    return *ptr_;
  }

  auto operator*() const -> T const& {
    TENZIR_ASSERT(ptr_);
    return *ptr_;
  }

private:
  explicit Box(std::unique_ptr<T> ptr) : ptr_{std::move(ptr)} {
    TENZIR_ASSERT(ptr_);
  }

  std::unique_ptr<T> ptr_;
};

template <class T>
Box(T ptr) -> Box<T>;

template <class T>
using Task = folly::coro::Task<T>;

template <class T>
class MutexGuard;

template <class T>
class Mutex {
public:
  explicit Mutex(T x) : value_{std::move(x)} {
  }

  auto lock() -> Task<MutexGuard<T>>;

private:
  friend class MutexGuard<T>;

  folly::coro::Mutex mutex_;
  T value_;
};

template <class T>
class MutexGuard {
public:
  ~MutexGuard() noexcept {
    maybe_unlock();
  }

  MutexGuard(MutexGuard&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(MutexGuard&& other) noexcept -> MutexGuard& {
    maybe_unlock();
    locked_ = other.locked_;
    other.locked_ = nullptr;
    return *this;
  }
  MutexGuard(MutexGuard& other) = delete;
  auto operator=(MutexGuard& other) = delete;

  auto operator*() -> T& {
    TENZIR_ASSERT(locked_);
    return locked_->value_;
  }

  auto operator->() -> T* {
    TENZIR_ASSERT(locked_);
    return &locked_->value_;
  }

  auto unlock() -> void {
    TENZIR_ASSERT(locked_);
    locked_->mutex_.unlock();
    locked_ = nullptr;
  }

private:
  auto maybe_unlock() -> void {
    if (locked_) {
      locked_->mutex_.unlock();
    }
  }

  friend class Mutex<T>;

  explicit MutexGuard(Mutex<T>& mutex) : locked_{&mutex} {
  }

  Mutex<T>* locked_ = nullptr;
};

template <class T>
auto Mutex<T>::lock() -> Task<MutexGuard<T>> {
  co_await mutex_.co_lock();
  co_return MutexGuard<T>{*this};
}

class AsyncCtx {
public:
  explicit AsyncCtx(caf::actor_system& sys) : sys_{sys} {
  }

  virtual ~AsyncCtx() = default;

  explicit(false) operator diagnostic_handler&() {
    return dh_;
  }

  auto caf() -> caf::actor_system& {
    return sys_;
  }

private:
  caf::actor_system& sys_;
  null_diagnostic_handler dh_;
};

template <class T>
class Push {
public:
  virtual ~Push() = default;

  virtual auto operator()(T output) -> Task<void> = 0;
};

enum class OperatorState {
  /// The operator doesn't request any specific state.
  unspecified,
  /// The operator doesn't want any more input.
  no_more_input,
};

class CheckpointId {};

// TODO: This interface doesn't work with source operators.
template <class Input, class Output>
class Operator {
public:
  virtual ~Operator() = default;

  // TODO: Do we rather want to expose an interface to wait for multiple
  // futures? The problem is that if we restore after a failure, we need to
  // restore the task that we were waiting on. Thus, forcing there to exist a
  // single task that is derived from state looks like a good idea.
  virtual auto await_task() const -> Task<std::any> {
    // We craft a task that will never complete on purpose.
    co_await folly::coro::sleep(folly::HighResDuration::max());
    TENZIR_UNREACHABLE();
  }

  virtual auto
  process_task(std::any result, Push<Output>& push, AsyncCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(result, push, ctx);
    TENZIR_UNREACHABLE();
  }

  virtual auto process(Input input, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void>
    = 0;

  virtual auto finalize(Push<Output>& push, AsyncCtx& ctx) -> Task<void> {
    co_return;
  }

  virtual auto checkpoint() -> Task<void> {
    // TODO: This should be implemented through `inspect`, right?
    co_return;
  }

  virtual auto post_commit() -> Task<void> {
    co_return;
  }

  virtual auto state() -> OperatorState {
    return OperatorState::unspecified;
  }
};

// TODO: Integrate this with the above?
template <class Input>
class Operator<Input, void> {
public:
  virtual ~Operator() = default;

  virtual auto process(Input input, AsyncCtx& ctx) -> Task<void> = 0;

  virtual auto checkpoint() -> Task<void> {
    // TODO: This should be implemented through `inspect`, right?
    co_return;
  }

  virtual auto state() -> OperatorState {
    return OperatorState::unspecified;
  }
};

template <class Output>
class Operator<void, Output> {
public:
  virtual ~Operator() = default;

  /// Return a task that is used as the input of the next `process` call once it
  /// completes. Checkpoints can be performed whenever `process` is not running.
  /// The task must therefore not modify state relevant for checkpointing.
  ///
  /// Returning an empty value denotes that the source is done.
  virtual auto next() const -> Task<std::any> = 0;

  virtual auto process(std::any result, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void>
    = 0;

  virtual auto checkpoint() -> Task<void> {
    // TODO: This should be implemented through `inspect`, right?
    co_return;
  }

  virtual auto post_commit() -> Task<void> {
    co_return;
  }
};

/// An easier interface for `Operator<void, Output>`.
template <class Output, class Step>
class SourceOperator {
public:
  virtual ~SourceOperator() = default;

  virtual auto next() const -> Task<std::optional<Step>> = 0;

  virtual auto process(Step result, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void>
    = 0;

  virtual auto checkpoint() -> Task<void> {
    // TODO: This should be implemented through `inspect`, right?
    co_return;
  }

  virtual auto post_commit() -> Task<void> {
    co_return;
  }
};

template <class Output, class Step>
class SourceOperatorWrapper : public Operator<void, Output> {
public:
  explicit SourceOperatorWrapper(Box<SourceOperator<Output, Step>> op)
    : op_{std::move(op)} {
  }

  auto next() const -> Task<std::any> override {
    auto result = co_await op_->next();
    if (result.has_value()) {
      co_return std::make_any<Step>(std::move(*result));
    }
    co_return {};
  }

  auto process(std::any result, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void> override {
    auto cast = std::any_cast<Step>(&result);
    TENZIR_ASSERT(cast);
    return op_->process(std::move(*cast), push, ctx);
  }

  auto checkpoint() -> Task<void> override {
    return op_->checkpoint();
  }

  auto post_commit() -> Task<void> override {
    return op_->post_commit();
  }

private:
  Box<SourceOperator<Output, Step>> op_;
};

using AnyOperator = variant<
  Box<Operator<void, chunk_ptr>>, Box<Operator<void, table_slice>>,
  Box<Operator<chunk_ptr, chunk_ptr>>, Box<Operator<chunk_ptr, table_slice>>,
  Box<Operator<table_slice, chunk_ptr>>, Box<Operator<table_slice, table_slice>>,
  Box<Operator<table_slice, void>>, Box<Operator<chunk_ptr, void>>>;

template <class SemiAwaitable, class F>
auto map_awaitable(SemiAwaitable&& awaitable, F&& f) -> Task<
  std::invoke_result_t<F, folly::coro::semi_await_result_t<SemiAwaitable>>> {
  co_return std::invoke(f, co_await std::forward<SemiAwaitable>(awaitable));
}

// TODO: This might not be cancellation safe?
template <class... SemiAwaitable>
auto select_into_variant(SemiAwaitable&&... awaitable)
  -> Task<variant<folly::coro::semi_await_result_t<SemiAwaitable>...>> {
  using result_t = variant<folly::coro::semi_await_result_t<SemiAwaitable>...>;
  auto [_, result] = co_await folly::coro::collectAny(
    map_awaitable(std::forward<SemiAwaitable>(awaitable), []<class T>(T&& x) {
      return result_t{std::forward<T>(x)};
    })...);
  TENZIR_ASSERT(result.hasValue());
  co_return std::move(*result);
}

/// A sequence of operators with the given input and output.
template <class Input, class Output>
class OperatorChain {
public:
  static auto try_from(std::vector<AnyOperator> operators)
    -> std::optional<OperatorChain<Input, Output>> {
    // TODO: Implement properly.
    return OperatorChain{std::move(operators)};
  }

  auto size() const -> size_t {
    return operators_.size();
  }

  auto operator[](size_t index) const -> const AnyOperator& {
    return operators_[index];
  }

  auto unwrap() && -> std::vector<AnyOperator> {
    return std::move(operators_);
  }

private:
  explicit OperatorChain(std::vector<AnyOperator> operators)
    : operators_{std::move(operators)} {
  }

  std::vector<AnyOperator> operators_;
};

enum class Signal {
  /// No more data will come after this signal.
  end_of_data,
  /// Request to perform a checkpoint.
  checkpoint,
};

template <class T>
struct OperatorMessage : variant<T, Signal> {
  using variant<T, Signal>::variant;
};

template <>
struct OperatorMessage<void> : variant<Signal> {
  using variant<Signal>::variant;
};

template <class T>
static auto cost(const OperatorMessage<T>& item) -> size_t {
  return match(
    item,
    [](const table_slice& slice) -> size_t {
      return slice.rows();
    },
    [](const chunk_ptr& chunk) -> size_t {
      return chunk ? chunk->size() : 0;
    },
    [](const Signal&) {
      return size_t{1};
    });
};

template <class T>
class OpSender;

template <class T>
class OpReceiver;

/// Data channel between two operators.
template <class T>
struct OpChannel {
public:
  explicit OpChannel(size_t limit) : mutex_{Locked{limit}} {
  }

  auto send(OperatorMessage<T> x) -> Task<void> {
    auto lock = co_await mutex_.lock();
    while (cost(x) > lock->remaining) {
      lock.unlock();
      co_await remaining_increased_;
      remaining_increased_.reset();
      lock = co_await mutex_.lock();
    }
    lock->remaining -= cost(x);
    lock->queue.push_back(x);
    queue_pushed_.post();
  }

  auto receive() -> Task<OperatorMessage<T>> {
    auto lock = co_await mutex_.lock();
    while (lock->queue.empty()) {
      lock.unlock();
      co_await queue_pushed_;
      queue_pushed_.reset();
      lock = co_await mutex_.lock();
    }
    auto result = std::move(lock->queue.front());
    lock->queue.pop_front();
    lock->remaining += cost(result);
    remaining_increased_.post();
    co_return result;
  }

private:
  struct Locked {
    explicit Locked(size_t limit) : remaining{limit} {
    }

    size_t remaining;
    std::deque<OperatorMessage<T>> queue;
  };

  // TODO: This can surely be written better?
  Mutex<Locked> mutex_;
  folly::coro::Baton remaining_increased_;
  folly::coro::Baton queue_pushed_;
};

template <class T>
class OpSender {
public:
  explicit OpSender(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  auto send(OperatorMessage<T> x) -> Task<void> {
    return shared_->send(std::move(x));
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
class OpReceiver {
public:
  explicit OpReceiver(std::shared_ptr<OpChannel<T>> shared)
    : shared_{std::move(shared)} {
  }

  auto receive() -> Task<OperatorMessage<T>> {
    return shared_->receive();
  }

private:
  std::shared_ptr<OpChannel<T>> shared_;
};

template <class T>
auto make_op_channel(size_t limit) -> std::pair<OpSender<T>, OpReceiver<T>> {
  auto shared = std::make_shared<OpChannel<T>>(limit);
  return {OpSender<T>{shared}, OpReceiver<T>{shared}};
}

auto run_pipeline(OperatorChain<void, void> pipeline, OpReceiver<void> input,
                  OpSender<void> output, caf::actor_system& sys) -> Task<void>;

} // namespace tenzir
