//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/routes_manager_actor.hpp"

#include <tenzir/node.hpp>
#include <tenzir/plugin.hpp>
#include <caf/actor_from_state.hpp>

namespace tenzir::plugins::routes {

class routes_manager_plugin final : public virtual component_plugin {
public:
  auto name() const -> std::string override {
    return "routes-manager";
  };

  auto make_component(node_actor::stateful_pointer<node_state> self) const
    -> component_plugin_actor override {
    // TODO: Shutdown order. Make pipeline manager depend on this.
    auto [fs] = self->state().registry.find<filesystem_actor>();
    return self->spawn<caf::linked>(caf::actor_from_state<routes_manager>,
                                    std::move(fs));
  }
};

} // namespace tenzir::plugins::routes

TENZIR_REGISTER_PLUGIN(tenzir::plugins::routes::routes_manager_plugin)
