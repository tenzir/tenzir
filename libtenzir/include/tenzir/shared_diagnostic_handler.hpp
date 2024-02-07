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
  shared_diagnostic_handler(const shared_diagnostic_handler&) = default;
  shared_diagnostic_handler& operator=(const shared_diagnostic_handler&)
    = default;
  shared_diagnostic_handler(shared_diagnostic_handler&&) noexcept = default;
  shared_diagnostic_handler& operator=(shared_diagnostic_handler&&) noexcept
    = default;

  inline shared_diagnostic_handler(const exec_node_actor& exec_node) noexcept
    : weak_exec_node_{exec_node} {
  }

  ~shared_diagnostic_handler() noexcept override = default;

  inline auto emit(diagnostic diag) -> void override {
    std::as_const(*this).emit(std::move(diag));
  }

  inline auto emit(diagnostic diag) const -> void {
    if (auto exec_node = weak_exec_node_.lock()) {
      caf::anon_send<caf::message_priority::high>(exec_node, std::move(diag));
    }
  }

  friend auto inspect(auto& f, shared_diagnostic_handler& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.shared_diagnostic_handler")
      .fields(f.field("weak_exec_node", x.weak_exec_node_));
  }

private:
  detail::weak_handle<exec_node_actor> weak_exec_node_ = {};
};

} // namespace tenzir
