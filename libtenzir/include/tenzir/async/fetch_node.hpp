//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/async/mail.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/diagnostics.hpp"

namespace tenzir {

/// Connects to the node, caching the result process-wide.
auto fetch_node(caf::actor_system& sys, diagnostic_handler& dh)
  -> Task<failure_or<node_actor>>;

template <class Actor>
auto fetch_actor_from_node(std::string_view name, location loc,
                           caf::actor_system& sys, diagnostic_handler& dh)
  -> Task<failure_or<Actor>> {
  CO_TRY(auto node, co_await fetch_node(sys, dh));
  auto names = std::vector<std::string>{};
  names.emplace_back(name);
  auto result
    = co_await async_mail(atom::get_v, atom::label_v, std::move(names))
        .request(node);
  if (not result) {
    diagnostic::error(result.error())
      .primary(loc)
      .note("failed to retrieve {} component from node", name)
      .emit(dh);
    co_return failure::promise();
  }
  if (result->size() != 1) {
    diagnostic::error("expected exactly one {} component, but "
                      "got {}",
                      name, result->size())
      .primary(loc)
      .emit(dh);
    co_return failure::promise();
  }
  auto casted = caf::actor_cast<Actor>(result->front());
  ;
  if (not casted) {
    diagnostic::error("failed to get {} component from node", name)
      .note("actor cast failed")
      .primary(loc)
      .emit(dh);
    co_return failure::promise();
  }
  co_return casted;
}

} // namespace tenzir
