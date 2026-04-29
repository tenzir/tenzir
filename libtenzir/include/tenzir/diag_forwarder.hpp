//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/diagnostics.hpp"

#include <caf/result.hpp>

namespace tenzir {

/// A CAF actor state that receives diagnostics and forwards them to a
/// `diagnostic_handler`. Useful for passing diagnostics across actor boundaries
/// via `shared_diagnostic_handler`.
struct DiagForwarder {
  [[maybe_unused]] static constexpr auto name = "diag-forwarder";

  explicit DiagForwarder(diagnostic_handler& dh) : dh_{dh} {
  }

  auto make_behavior() -> receiver_actor<diagnostic>::behavior_type {
    return {
      [this](diagnostic d) -> caf::result<void> {
        dh_.emit(std::move(d));
        return {};
      },
    };
  }

  diagnostic_handler& dh_;
};

} // namespace tenzir
