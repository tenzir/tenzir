//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/mutex.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

#include <caf/actor_cast.hpp>
#include <caf/mailbox_element.hpp>
#include <caf/response_type.hpp>
#include <folly/coro/Collect.h>
#include <folly/coro/Mutex.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Synchronized.h>
#include <folly/coro/Task.h>
#include <folly/coro/Traits.h>
#include <folly/futures/Future.h>

#include <any>

namespace tenzir {

template <class T>
using AsyncGenerator = folly::coro::AsyncGenerator<T&&>;

template <class Result, class Handle, class F>
void mail_with_callback(Handle receiver, caf::message msg, F f) {
  auto companion = receiver->home_system().make_companion();
  // We need to wrap non-copyable functions because CAF wants a copy...
  companion->on_enqueue(
    [f = std::make_shared<F>(std::move(f))](caf::mailbox_element_ptr ptr) {
      if (ptr->payload.match_elements<caf::error>()) {
        std::invoke(std::move(*f),
                    caf::expected<Result>{ptr->payload.get_as<caf::error>(0)});
        return;
      }
      if (ptr->payload.match_element<Result>(0)) {
        std::invoke(std::move(*f),
                    caf::expected<Result>{ptr->payload.get_as<Result>(0)});
        return;
      }
      // TODO: Apparently we cannot throw here?
      TENZIR_ERROR("OH NO");
      return;
    });
  companion->mail(std::move(msg)).send(caf::actor_cast<caf::actor>(receiver));
};

template <class Handle, class... Args>
using AsyncMailResult = caf::expected<caf::detail::tl_head_t<
  caf::response_type_t<typename Handle::signatures, std::decay_t<Args>...>>>;

template <class... Args>
class AsyncMail {
public:
  explicit AsyncMail(caf::message msg) : msg_{std::move(msg)} {
  }

  template <class Handle, class Result = AsyncMailResult<Handle, Args...>>
  auto request(Handle receiver) && -> folly::SemiFuture<Result> {
    auto [promise, future] = folly::makePromiseContract<Result>();
    mail_with_callback<typename Result::value_type>(
      receiver, std::move(msg_),
      [promise = std::move(promise)](Result result) mutable {
        promise.setValue(std::move(result));
      });
    return std::move(future);
  }

private:
  caf::message msg_;
};

class AsyncCtx {
public:
  explicit AsyncCtx(caf::actor_system& sys) : sys_{sys} {
  }

  virtual ~AsyncCtx() = default;

  explicit(false) operator diagnostic_handler&() {
    return dh_;
  }

  auto actor_system() -> caf::actor_system& {
    return sys_;
  }

  template <class... Ts>
  auto mail(Ts&&... xs) -> AsyncMail<std::decay_t<Ts>...> {
    return AsyncMail<std::decay_t<Ts>...>{
      caf::make_message(std::forward<Ts>(xs)...)};
  }

private:
  caf::actor_system& sys_;
  null_diagnostic_handler dh_;
};

/// A type-erased, asynchronous sender.
template <class T>
class Push {
public:
  virtual ~Push() = default;

  virtual auto operator()(T output) -> Task<void> = 0;
};

/// A type-erased, asynchronous receiver.
template <class T>
class Pull {
public:
  virtual ~Pull() = default;

  virtual auto operator()() -> Task<T> = 0;
};

/// A pair of a type-erased, asynchronous sender and receiver.
template <class T>
struct PushPull {
  Box<Push<T>> push;
  Box<Pull<T>> pull;
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

  virtual auto process_task(std::any result, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void> {
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

/// A non-data message sent to an operator by its upstream.
enum class Signal {
  /// No more data will come after this signal. Will never be sent to sources.
  end_of_data,
  /// Request to perform a checkpoint. To be forwarded downstream afterwards.
  checkpoint,
};

template <class T>
struct OperatorMsg : variant<T, Signal> {
  using variant<T, Signal>::variant;
};

template <>
struct OperatorMsg<void> : variant<Signal> {
  using variant<Signal>::variant;
};

template <class T>
auto make_op_channel(size_t limit) -> PushPull<OperatorMsg<T>>;

auto run_pipeline(OperatorChain<void, void> pipeline,
                  Box<Pull<OperatorMsg<void>>> input,
                  Box<Push<OperatorMsg<void>>> output, caf::actor_system& sys)
  -> Task<void>;

} // namespace tenzir
