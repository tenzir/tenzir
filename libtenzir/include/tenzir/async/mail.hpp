//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/error.hpp"

#include <caf/actor_cast.hpp>
#include <caf/actor_companion.hpp>
#include <caf/actor_registry.hpp>
#include <caf/mailbox_element.hpp>
#include <caf/message.hpp>
#include <caf/response_type.hpp>
#include <caf/timespan.hpp>
#include <caf/unit.hpp>
#include <folly/futures/Future.h>

#include <atomic>
#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace tenzir {

template <class Result, class Handle, class F, class... Ts>
void mail_with_callback(Handle receiver, F f, Ts&&... xs) {
  auto companion = receiver->home_system().make_companion();
  auto& registry = companion->home_system().registry();
  auto companion_id = companion->id();
  auto* companion_ptr = companion.get();
  auto request_id = companion->new_request_id(caf::message_priority::normal);
  auto response_id = request_id.response_id();
  auto completed = std::make_shared<std::atomic<bool>>(false);
  auto fn = std::make_shared<F>(std::move(f));
  auto finish
    = [fn, completed, &registry, companion_id](caf::expected<Result> res) {
        auto expected = false;
        if (not completed->compare_exchange_strong(expected, true,
                                                   std::memory_order_relaxed)) {
          return;
        }
        std::invoke(*fn, std::move(res));
        // Remove any registry entry that may keep the temporary companion
        // alive (e.g., when CAF registers it for remote routing).
        registry.erase(companion_id);
      };
  companion->on_exit([companion_ptr, finish] {
    finish(caf::expected<Result>{companion_ptr->fail_state()});
  });
  companion->on_enqueue([response_id,
                         finish](caf::mailbox_element_ptr ptr) mutable {
    TENZIR_ASSERT(ptr);
    if (ptr->payload.match_element<caf::down_msg>(0)) {
      auto dm = ptr->payload.get_as<caf::down_msg>(0);
      finish(caf::expected<Result>{std::move(dm.reason)});
      return;
    }
    if (ptr->mid != response_id) {
      return;
    }
    if (ptr->payload.match_elements<caf::error>()) {
      finish(caf::expected<Result>{ptr->payload.get_as<caf::error>(0)});
      return;
    }
    if constexpr (std::is_void_v<Result>) {
      if (ptr->payload.empty() || ptr->payload.match_element<caf::unit_t>(0)) {
        finish(caf::expected<void>{});
      } else {
        finish(caf::expected<void>{caf::make_error(
          ec::logic_error, "unexpected non-empty payload for void response")});
      }
    } else if (ptr->payload.match_element<Result>(0)) {
      finish(caf::expected<Result>{ptr->payload.get_as<Result>(0)});
    } else {
      finish(caf::expected<Result>{
        caf::make_error(ec::logic_error, "unexpected response payload type")});
    }
  });
  companion->monitor(receiver);
  auto* actor = caf::actor_cast<caf::abstract_actor*>(receiver);
  if (not actor) {
    finish(caf::expected<Result>{
      caf::make_error(ec::logic_error, "invalid receiver in async_mail")});
    return;
  }
  actor->enqueue(caf::make_mailbox_element(
                   {companion->ctrl(), caf::add_ref}, request_id,
                   caf::make_message_nowrap(std::forward<Ts>(xs)...)),
                 companion->context());
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
  auto request(Handle receiver) && -> folly::SemiFuture<Result> {
    auto [promise, future] = folly::makePromiseContract<Result>();
    std::apply(
      [&](auto&&... xs) mutable {
        mail_with_callback<typename Result::value_type>(
          receiver,
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
