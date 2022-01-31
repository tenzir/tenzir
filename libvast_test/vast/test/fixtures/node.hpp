//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/system/node.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/uuid.hpp"

#include <string>

namespace fixtures {

struct node : deterministic_actor_system_and_events {
  node(const std::string& suite);

  ~node() override;

  template <class... Ts>
  caf::actor spawn_component(std::string component, Ts&&... args) {
    using namespace caf;
    using namespace std::string_literals;
    actor result;
    invocation inv;
    inv.full_name = "spawn "s + component;
    inv.options = {};
    inv.arguments = {std::forward<Ts>(args)...};
    auto rh = self->request(test_node, infinite, atom::spawn_v, std::move(inv));
    run();
    rh.receive([&](const actor& a) { result = a; },
               [&](const error& e) {
                 FAIL("failed to spawn " << component << ": " << render(e));
               });
    return result;
  }

  // Ingests a specific type of logs.
  void ingest(const std::string& type);

  // Performs a historical query and returns the resulting events.
  std::vector<table_slice> query(std::string expr);

  system::node_actor test_node;
};

} // namespace fixtures
