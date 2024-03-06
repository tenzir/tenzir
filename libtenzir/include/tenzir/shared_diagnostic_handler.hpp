//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/weak_handle.hpp"
#include "tenzir/diagnostics.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

/// A diagnostic handler that may be passed to other threads from an operator.
class shared_diagnostic_handler final : public diagnostic_handler {
public:
  shared_diagnostic_handler() noexcept = default;

  template <typename... Sigs>
  shared_diagnostic_handler(const caf::typed_event_based_actor<Sigs...>& sender,
                            receiver_actor<diagnostic> receiver) noexcept
    : sender_{sender.ctrl()},
      receiver_{receiver},
      send_callback_{[](caf::strong_actor_ptr erased_sender,
                        receiver_actor<diagnostic> receiver, diagnostic diag) {
        // auto sender = caf::actor_cast<caf::typed_event_based_actor<Sigs...>>(
        //   erased_sender->get());
        auto* sender = static_cast<caf::typed_event_based_actor<Sigs...>*>(
          erased_sender->get());
        // sender->template send<caf::message_priority::high>(receiver,
        //                                                    std::move(diag));
        sender
          ->template request<caf::message_priority::high>(receiver,
                                                          caf::infinite, diag)
          .then([]() {},
                [/*receiver, diag*/](const caf::error& err) {
                  TENZIR_WARN("failed to deliver diagnostic: {}", err);
                  // caf::anon_send<caf::message_priority::high>(receiver, diag);
                });
      }} {
  }

  auto emit(diagnostic diag) -> void override {
    std::as_const(*this).emit(std::move(diag));
  }

  auto emit(diagnostic diag) const -> void {
    if (auto sender = sender_.lock()) {
      if (auto receiver = receiver_.lock()) {
        send_callback_(sender, receiver, std::move(diag));
      }
    }
    // send_callback_(sender_, receiver_, std::move(diag));
  }

private:
  caf::weak_actor_ptr sender_ = {};
  detail::weak_handle<receiver_actor<diagnostic>> receiver_ = {};
  // caf::strong_actor_ptr sender_ = {};
  // receiver_actor<diagnostic> receiver_ = {};
  std::function<
    auto(caf::strong_actor_ptr, receiver_actor<diagnostic>, diagnostic)->void>
    send_callback_ = {};
};

} // namespace tenzir

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(tenzir::shared_diagnostic_handler)
