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

#include "vast/error.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/sink.hpp"

#define SUITE system
#include "vast/test/test.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(sink_tests, fixtures::actor_system_and_events)

TEST(zeek sink) {
  MESSAGE("constructing a sink");
  auto writer = std::make_unique<format::zeek::writer>(directory, false);
  auto snk = self->spawn(sink, std::move(writer), 20u);
  MESSAGE("sending table slices");
  for (auto& slice : zeek_conn_log)
    self->send(snk, slice);
  MESSAGE("shutting down");
  self->send_exit(snk, caf::exit_reason::user_shutdown);
  self->wait_for(snk);
  CHECK(exists(directory / "zeek.conn.log"));
}

FIXTURE_SCOPE_END()
