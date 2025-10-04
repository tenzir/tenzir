//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/typed_event_based_actor.hpp>

#include <queue>

namespace tenzir::detail {

class request_cache {
public:
  template <class... Signatures, class... Args>
  auto
  stash(caf::typed_event_based_actor<Signatures...>* self, Args&&... args) {
    using handle_type = caf::typed_actor<Signatures...>;
    using response_type = caf::response_type_t<typename handle_type::signatures,
                                               std::remove_cvref_t<Args>...>;
    return [&]<class... Ts>(caf::type_list<Ts...>) {
      auto rp = self->template make_response_promise<Ts...>();
      stash_.emplace(
        [self, rp,
         args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          TENZIR_ASSERT(rp.pending());
          std::apply(
            [&](auto&&... args) {
              rp.delegate(handle_type{self},
                          std::forward<decltype(args)>(args)...);
            },
            std::move(args));
        });
      return rp;
    }.template operator()(response_type{});
  }

  auto unstash() {
    while (not stash_.empty()) {
      stash_.front()();
      stash_.pop();
    }
  }

private:
  std::queue<std::function<void()>> stash_;
};

} // namespace tenzir::detail
