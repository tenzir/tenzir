//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/error.hpp"

#include <caf/actor_cast.hpp>
#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/message.hpp>
#include <caf/response_type.hpp>
#include <caf/timespan.hpp>
#include <caf/unit.hpp>
#include <folly/futures/Future.h>

#include <atomic>
#include <functional>
#include <memory>
#include <source_location>
#include <tuple>
#include <type_traits>
#include <utility>

namespace tenzir {

template <class Result, class Handle, class F, class... Ts>
void mail_with_callback(Handle receiver, std::source_location location, F f,
                        Ts&&... xs) {
  struct state_t {
    explicit state_t(F fn) : callback{std::move(fn)} {
    }

    auto finish(caf::expected<Result> result) -> void {
      auto expected = false;
      if (not completed.compare_exchange_strong(expected, true,
                                                std::memory_order_relaxed)) {
        return;
      }
      std::invoke(callback, std::move(result));
    }

    std::atomic<bool> completed = false;
    F callback;
  };
  if (not receiver) {
    std::invoke(f, caf::expected<Result>{caf::make_error(
                     ec::remote_node_down, "async_mail receiver down")});
    return;
  }
  auto state = std::make_shared<state_t>(std::move(f));
  receiver->home_system().template spawn<caf::hidden>(
    [receiver = std::move(receiver), location, state = std::move(state),
     args = std::make_tuple(std::forward<Ts>(xs)...)](
      caf::event_based_actor* self) mutable -> caf::behavior {
      self->attach_functor([location, state] {
        state->finish(caf::expected<Result>{
          caf::make_error(ec::logic_error,
                          fmt::format("async_mail helper terminated before "
                                      "producing a response for request at "
                                      "{}:{}",
                                      location.file_name(), location.line()))});
      });
      std::apply(
        [self, &receiver, &state](auto&&... ys) mutable {
          if constexpr (std::is_void_v<Result>) {
            self->mail(std::move(ys)...)
              .request(receiver, caf::infinite)
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
              .request(receiver, caf::infinite)
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
  request(Handle receiver,
          std::source_location location
          = std::source_location::current()) and -> folly::SemiFuture<Result> {
    auto [promise, future] = folly::makePromiseContract<Result>();
    std::apply(
      [&](auto&&... xs) mutable {
        mail_with_callback<typename Result::value_type>(
          receiver, location,
          [promise = std::move(promise)](Result result) mutable {
            promise.setValue(std::move(result));
          },
          std::move(xs)...);
      },
      std::move(args_));
    return std::move(future);
  }

private:
  std::tuple<Args...> args_;
};

template <class... Ts>
auto async_mail(Ts&&... xs) -> AsyncMail<std::decay_t<Ts>...> {
  return AsyncMail<std::decay_t<Ts>...>{std::forward<Ts>(xs)...};
}

} // namespace tenzir
