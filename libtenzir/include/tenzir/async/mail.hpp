//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/logger.hpp"

#include <caf/actor_cast.hpp>
#include <caf/actor_companion.hpp>
#include <caf/actor_registry.hpp>
#include <caf/mailbox_element.hpp>
#include <caf/response_type.hpp>
#include <caf/unit.hpp>
#include <folly/futures/Future.h>
#include <type_traits>

namespace tenzir {

template <class Result, class Handle, class F>
void mail_with_callback(Handle receiver, caf::message msg, F f) {
  auto companion = receiver->home_system().make_companion();
  auto& registry = companion->home_system().registry();
  auto companion_id = companion->id();
  // We need to wrap non-copyable functions because CAF wants a copy...
  auto handled = std::make_shared<bool>(false);
  companion->on_enqueue([f = std::make_shared<F>(std::move(f)), handled,
                         &registry,
                         companion_id](caf::mailbox_element_ptr ptr) {
    // Only process the first message (the response).
    if (*handled) {
      return;
    }
    *handled = true;
    if (ptr->payload.match_elements<caf::error>()) {
      std::invoke(std::move(*f),
                  caf::expected<Result>{ptr->payload.get_as<caf::error>(0)});
    } else if constexpr (std::is_void_v<Result>) {
      if (ptr->payload.empty() || ptr->payload.match_element<caf::unit_t>(0)) {
        std::invoke(std::move(*f), caf::expected<void>{});
      } else {
        TENZIR_ERROR("unexpected non-empty payload for void response");
      }
    } else if (ptr->payload.match_element<Result>(0)) {
      std::invoke(std::move(*f),
                  caf::expected<Result>{ptr->payload.get_as<Result>(0)});
    } else {
      // TODO: Apparently we cannot throw here?
      TENZIR_ERROR("unexpected response payload type");
    }
    // Serializing the companion for network transmission registers it.
    // Erase it to release that reference and allow cleanup.
    registry.erase(companion_id);
  });
  companion->mail(std::move(msg)).send(caf::actor_cast<caf::actor>(receiver));
}

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

template <class... Ts>
auto async_mail(Ts&&... xs) -> AsyncMail<std::decay_t<Ts>...> {
  return AsyncMail<std::decay_t<Ts>...>{
    caf::make_message(std::forward<Ts>(xs)...)};
}

} // namespace tenzir
