//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#pragma once

#include "routes/config.hpp"
#include "routes/proxy_actor.hpp"
#include "tenzir/atoms.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/status.hpp>
#include <tenzir/table_slice.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::routes {

struct routes_manager_actor_traits {
  using signatures = caf::type_list<
    auto(atom::add, named_input_actor input)->caf::result<void>,
    auto(atom::add, named_output_actor output)->caf::result<void>,
    auto(atom::update, config cfg)->caf::result<void>,
    auto(atom::list)
      ->caf::result<config>>::append_from<component_plugin_actor::signatures>;
};

using routes_manager_actor = caf::typed_actor<routes_manager_actor_traits>;

class routes_manager {
public:
  [[maybe_unused]] static constexpr auto name = "routes-manager";

  explicit routes_manager(routes_manager_actor::pointer self,
                          filesystem_actor fs);
  auto make_behavior() -> routes_manager_actor::behavior_type;

private:
  // TODO: Check uniqueness for inputs?
  // TODO: Ensure uniqueness of inputs throughout different pipelines (done?)
  // and routes (route <-> route, pipeline <-> route). Change unordered map
  // variant<proxy_actor, vector<rule>>?
  // TODO: Consider storing this in a a vector and allowing duplicate
  // input/output names, as there's little reason to forbid them. Just need to
  // adapt the monitor calls to go by actor ID then.
  auto add(named_input_actor input) -> caf::result<void>;
  auto add(named_output_actor output) -> caf::result<void>;
  auto update(config cfg) -> caf::result<void>;
  auto list() -> caf::result<config>;

  auto _restore_state() -> void;
  auto _run_for(input input_name) -> void;
  auto _find_outputs(const input& input_name) -> std::vector<output>;
  auto _forward(const output& output_name, table_slice slice) -> void;
  auto _inline_forward_to_outputs(const output& output_name, table_slice slice)
    -> void;
  auto _inline_forward_to_routes(const output& output_name, table_slice slice)
    -> void;

  routes_manager_actor::pointer self_ = {};
  filesystem_actor fs_ = {};

  config cfg_;

  // Maps input and output names to their respective proxy actors, whch can be
  // either a proxy spawned by an input or output operator, or a route.
  // TODO: Currently, all evaluation of rules is happening inside the
  // route-manager actor directly. This may prove to be a bottleneck, but the
  // architecture can easily be extended in a way where one actor is spawned per
  // route to distribute the load.
  std::unordered_map<input, proxy_actor> inputs_;
  std::unordered_map<output, proxy_actor> outputs_;
};

} // namespace tenzir::plugins::routes
