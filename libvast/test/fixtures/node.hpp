/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <caf/all.hpp>

#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/uuid.hpp"

#include "vast/system/node.hpp"
#include "vast/system/query_statistics.hpp"

#include "data.hpp"
#include "fixtures/actor_system_and_events.hpp"

namespace fixtures {

struct node : deterministic_actor_system_and_events {
  node();

  ~node() override;

  template <class... Ts>
  caf::actor spawn_component(std::string component, Ts&&... args) {
    using namespace caf;
    actor result;
    auto msg = make_message(std::move(component), std::forward<Ts>(args)...);
    auto rh = self->request(test_node, infinite, "spawn", std::move(msg));
    run();
    rh.receive(
      [&](const actor& a) { result = a; },
      [&](const error& e) {
        FAIL("failed to spawn " << component << ": " << sys.render(e));
       }
    );
    return result;
  }

  // Ingests a specific type of logs.
  void ingest(const std::string& type);

  // Performs a historical query and returns the resulting events.
  std::vector<event> query(std::string expr);

  caf::actor test_node;
};

} // namespace fixtures

