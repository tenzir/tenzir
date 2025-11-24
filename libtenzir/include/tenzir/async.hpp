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
#include "tenzir/async/push_pull.hpp"
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
  AsyncCtx(caf::actor_system& sys, diagnostic_handler& dh)
    : sys_{sys}, dh_{dh} {
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
  diagnostic_handler& dh_;
};

enum class OperatorState {
  /// The operator doesn't request any specific state.
  unspecified,
  /// The operator wants to finalize.
  done,
};

class CheckpointId {};

template <class Input, class Output>
class OperatorInputOutputBase {
public:
  virtual auto process(Input input, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void>
    = 0;

protected:
  ~OperatorInputOutputBase() = default;
};

template <class Output>
class OperatorInputOutputBase<void, Output> {};

template <class Input>
class OperatorInputOutputBase<Input, void> {
public:
  virtual auto process(Input input, AsyncCtx& ctx) -> Task<void> = 0;

protected:
  ~OperatorInputOutputBase() = default;
};

template <class Output>
class OperatorOutputBase {
public:
  virtual auto start(Push<Output>& push, AsyncCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(push, ctx);
    co_return;
  }

  virtual auto process_task(std::any result, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void> {
    TENZIR_UNUSED(result, push, ctx);
    TENZIR_ERROR("ignoring task result in {}", typeid(*this).name());
    co_return;
  }

  virtual auto finalize(Push<Output>& push, AsyncCtx& ctx) -> Task<void> {
    co_return;
  }

protected:
  ~OperatorOutputBase() = default;
};

template <>
class OperatorOutputBase<void> {
public:
  virtual auto start(AsyncCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  virtual auto process_task(std::any result, AsyncCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(result, ctx);
    TENZIR_ERROR("ignoring task result in {}", typeid(*this).name());
    co_return;
  }

  virtual auto finalize(AsyncCtx& ctx) -> Task<void> {
    co_return;
  }

protected:
  ~OperatorOutputBase() = default;
};

template <class Input, class Output>
class Operator : public OperatorInputOutputBase<Input, Output>,
                 public OperatorOutputBase<Output> {
public:
  virtual ~Operator() = default;

  // TODO: Do we rather want to expose an interface to wait for multiple
  // futures? The problem is that if we restore after a failure, we need to
  // restore the task that we were waiting on. Thus, forcing there to exist a
  // single task that is derived from state looks like a good idea.
  virtual auto await_task() const -> Task<std::any> {
    // We craft a task that will never complete on purpose.
    co_await folly::coro::sleep(std::chrono::years{1});
    TENZIR_ERROR("TODO");
    TENZIR_UNREACHABLE();
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

  auto await_task() const -> Task<std::any> override {
    auto result = co_await op_->next();
    if (result.has_value()) {
      co_return std::make_any<Step>(std::move(*result));
    }
    co_return {};
  }

  auto process_task(std::any result, Push<Output>& push, AsyncCtx& ctx)
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
    auto input = operator_type{tag_v<Input>};
    for (auto& op : operators) {
      TRY(input, match(op,
                       [&]<class In, class Out>(Box<Operator<In, Out>>&)
                         -> std::optional<operator_type> {
                         if (not input.is<In>()) {
                           return std::nullopt;
                         }
                         return tag_v<Out>;
                       }));
    }
    if (not input.is<Output>()) {
      return std::nullopt;
    }
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

TENZIR_ENUM(
  /// A non-data message sent to an operator by its upstream.
  Signal,
  /// No more data will come after this signal. Will never be sent over `void`.
  end_of_data,
  /// Request to perform a checkpoint. To be forwarded downstream afterwards.
  checkpoint);

template <class T>
struct OperatorMsg : variant<T, Signal> {
  using variant<T, Signal>::variant;
};

template <class T>
inline constexpr auto enable_default_formatter<OperatorMsg<T>> = true;

template <>
struct OperatorMsg<void> : variant<Signal> {
  using variant<Signal>::variant;
};

template <class T>
auto make_op_channel(size_t limit) -> PushPull<OperatorMsg<T>>;

struct PostCommit {};
struct Shutdown {};
struct StopOutput {};

using FromControl = variant<PostCommit, Shutdown, StopOutput>;

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

// TODO: Where to place this?
template <class T>
class Receiver;
template <class T>
class Sender;

class Never {
private:
  Never() = default;
};

template <class T>
using VoidToNever = std::conditional_t<std::same_as<T, void>, Never, T>;

/// Run a closed pipeline without external control.
auto run_pipeline(OperatorChain<void, void> pipeline, caf::actor_system& sys,
                  diagnostic_handler& dh) -> Task<void>;

/// Run a right-open pipeline without external control.
template <class Output>
  requires(not std::same_as<Output, void>)
auto run_pipeline(OperatorChain<void, Output> pipeline, caf::actor_system& sys,
                  diagnostic_handler& dh) -> AsyncGenerator<Output>;

/// Run an open pipeline without external control.
template <class Input, class Output>
  requires(not std::same_as<Output, void> and not std::same_as<Input, void>)
auto run_pipeline_with_input(AsyncGenerator<Input> input,
                             OperatorChain<Input, Output> pipeline,
                             caf::actor_system& sys, diagnostic_handler& dh)
  -> AsyncGenerator<Output>;

/// Run a pipeline with external control.
template <class Input, class Output>
auto run_chain(OperatorChain<Input, Output> chain,
               Box<Pull<OperatorMsg<Input>>> pull_upstream,
               Box<Push<OperatorMsg<Output>>> push_downstream,
               Receiver<FromControl> from_control, Sender<ToControl> to_control,
               caf::actor_system& sys, diagnostic_handler& dh) -> Task<void>;

} // namespace tenzir
