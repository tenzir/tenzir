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
#include <folly/coro/Task.h>
#include <folly/coro/Traits.h>

#include <any>

namespace tenzir {

template <class T>
using Task = folly::coro::Task<T>;

template <class T>
class MutexGuard;

// TODO: I noticed `folly::coro::Synchronized` is similar.
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
    try_unlock();
  }

  MutexGuard(MutexGuard&& other) noexcept {
    *this = std::move(other);
  }
  auto operator=(MutexGuard&& other) noexcept -> MutexGuard& {
    try_unlock();
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
  auto try_unlock() -> void {
    if (locked_) {
      locked_->mutex_.unlock();
    }
  }

  friend class Mutex<T>;

  explicit MutexGuard(Mutex<T>& mutex) : locked_{&mutex} {
  }

  Mutex<T>* locked_;
};

template <class T>
auto Mutex<T>::lock() -> Task<MutexGuard<T>> {
  co_await mutex_.co_lock();
  co_return MutexGuard<T>{*this};
}

class AsyncCtx {
public:
  virtual ~AsyncCtx() = default;

  explicit(false) operator diagnostic_handler&() {
    return dh_;
  }

  auto caf() -> caf::actor_system&;

private:
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

  // TODO: Do we rather want to expose an interface to wait for multiple futures?
  virtual auto await_task() const -> Task<std::any> {
    // We craft a task that will never complete on purpose.
    co_await folly::coro::sleep(folly::HighResDuration::max());
    TENZIR_UNREACHABLE();
  }

  virtual auto
  process_task(std::any result, Push<Output>& push, AsyncCtx& ctx) -> Task<void> {
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

template <class Step, class Output>
class SourceOperatorWrapper : public Operator<void, Output> {
public:
  explicit SourceOperatorWrapper(
    std::unique_ptr<SourceOperator<Step, Output>> op)
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

  virtual auto checkpoint() -> Task<void> {
    return op_->checkpoint();
  }

  virtual auto post_commit() -> Task<void> {
    return op_->post_commit();
  }

private:
  std::unique_ptr<SourceOperator<Step, Output>> op_;
};

using OperatorPtr = variant<std::unique_ptr<Operator<void, chunk_ptr>>,
                            std::unique_ptr<Operator<void, table_slice>>,
                            std::unique_ptr<Operator<chunk_ptr, chunk_ptr>>,
                            std::unique_ptr<Operator<chunk_ptr, table_slice>>,
                            std::unique_ptr<Operator<table_slice, chunk_ptr>>,
                            std::unique_ptr<Operator<table_slice, table_slice>>,
                            std::unique_ptr<Operator<table_slice, void>>,
                            std::unique_ptr<Operator<chunk_ptr, void>>>;

template <class SemiAwaitable, class F>
auto map_awaitable(SemiAwaitable&& awaitable, F&& f) -> Task<
  std::invoke_result_t<F, folly::coro::semi_await_result_t<SemiAwaitable>>> {
  co_return std::invoke(f, co_await std::forward<SemiAwaitable>(awaitable));
}

// TODO: This might not be cancellation safe?
template <class... SemiAwaitable>
inline auto select_into_variant(SemiAwaitable&&... awaitable)
  -> Task<variant<folly::coro::semi_await_result_t<SemiAwaitable>...>> {
  using result_t = variant<folly::coro::semi_await_result_t<SemiAwaitable>...>;
  auto [_, result] = co_await folly::coro::collectAny(
    map_awaitable(std::forward<SemiAwaitable>(awaitable), []<class T>(T&& x) {
      return result_t{std::forward<T>(x)};
    })...);
  TENZIR_ASSERT(result.hasValue());
  co_return std::move(*result);
}

} // namespace tenzir
