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

#define SUITE spawn_indexer_actors

#include "vast/system/spawn_indexer_actors.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/table_slice.hpp"

using namespace vast;

FIXTURE_SCOPE(spawn_indexer_actors_tests,
              fixtures::deterministic_actor_system_and_events)

TEST(bro conn layout) {
  auto xs = system::spawn_indexer_actors(self.ptr(), directory,
                                         bro_conn_log_slices[0]->layout());
  CHECK(!xs.empty());
}

FIXTURE_SCOPE_END()
