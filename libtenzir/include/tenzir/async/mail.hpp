//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/future_util.hpp"
#include "tenzir/error.hpp"

#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/message.hpp>
#include <caf/response_type.hpp>
#include <caf/send.hpp>
#include <caf/timespan.hpp>
#include <caf/unit.hpp>
#include <folly/ExceptionWrapper.h>
#include <folly/futures/Future.h>

#include <atomic>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace tenzir {

template <class Result>
struct AsyncMailRequestState {
  using FutureResult = caf::expected<Result>;

  explicit AsyncMailRequestState(folly::Promise<FutureResult> promise)
    : promise{std::move(promise)} {
  }

  auto finish(FutureResult result) -> void {
    auto expected = false;
    if (not completed.compare_exchange_strong(expected, true,
                                              std::memory_order_relaxed)) {
      return;
    }
    promise.setValue(std::move(result));
  }

  auto cancel(folly::exception_wrapper const& exception) -> void {
    auto expected = false;
    if (not completed.compare_exchange_strong(expected, true,
                                              std::memory_order_relaxed)) {
      return;
    }
    promise.setException(exception);
    if (helper) {
      caf::anon_send_exit(helper, caf::exit_reason::user_shutdown);
    }
  }

  std::atomic<bool> completed = false;
  folly::Promise<FutureResult> promise;
  caf::actor helper;
};

template <class Result, class Handle, class... Ts>
auto mail_with_callback(Handle receiver, caf::timespan timeout,
                        std::shared_ptr<AsyncMailRequestState<Result>> state,
                        Ts&&... xs) -> caf::actor {
  if (not receiver) {
    state->finish(caf::expected<Result>{
      caf::make_error(ec::remote_node_down, "async_mail receiver down")});
    return {};
  }
  return receiver->home_system().template spawn<caf::hidden>(
    [receiver = std::move(receiver), timeout, state = std::move(state),
     args = std::make_tuple(std::forward<Ts>(xs)...)](
      caf::event_based_actor* self) mutable -> caf::behavior {
      std::apply(
        [self, &receiver, &state, timeout](auto&&... ys) mutable {
          if constexpr (std::is_void_v<Result>) {
            self->mail(std::move(ys)...)
              .request(receiver, timeout)
              .then(
                [self, state]() mutable {
                  state->finish(caf::expected<void>{});
                  self->quit();
                },
                [self, state](caf::error& err) mutable {
                  state->finish(caf::expected<void>{caf::error{err}});
                  self->quit(err);
                });
          } else {
            self->mail(std::move(ys)...)
              .request(receiver, timeout)
              .then(
                [self, state](Result result) mutable {
                  state->finish(caf::expected<Result>{std::move(result)});
                  self->quit();
                },
                [self, state](caf::error& err) mutable {
                  state->finish(caf::expected<Result>{caf::error{err}});
                  self->quit(err);
                });
          }
        },
        std::move(args));
      return {};
    });
}

template <class Handle, class... Args>
using AsyncMailResult = caf::expected<caf::detail::tl_head_t<
  caf::response_type_t<typename Handle::signatures, std::decay_t<Args>...>>>;

template <class... Args>
class AsyncMail {
public:
  explicit AsyncMail(Args... xs) : args_{std::move(xs)...} {
  }

  template <class Handle, class Result = AsyncMailResult<Handle, Args...>>
  auto
  request(Handle receiver, caf::timespan timeout
                           = caf::infinite) && -> folly::coro::Task<Result> {
    auto [promise, future] = folly::makePromiseContract<Result>();
    auto state
      = std::make_shared<AsyncMailRequestState<typename Result::value_type>>(
        std::move(promise));
    state->promise.setInterruptHandler(
      [weak_state = std::weak_ptr{state}](
        folly::exception_wrapper const& exception) mutable {
        if (auto state = weak_state.lock()) {
          state->cancel(exception);
        }
      });
    std::apply(
      [&](auto&&... xs) mutable {
        state->helper = mail_with_callback<typename Result::value_type>(
          receiver, timeout, state, std::move(xs)...);
      },
      std::move(args_));
    return to_task_interrupt_on_cancel(std::move(future));
  }

private:
  std::tuple<Args...> args_;
};

template <class... Ts>
auto async_mail(Ts&&... xs) -> AsyncMail<std::decay_t<Ts>...> {
  return AsyncMail<std::decay_t<Ts>...>{std::forward<Ts>(xs)...};
}

} // namespace tenzir
