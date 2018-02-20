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
#include "vast/format/bro.hpp"
#include "vast/system/sink.hpp"

#define SUITE system
#include "test.hpp"
#include "data.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(sink_tests, fixtures::actor_system_and_events)

TEST(Bro sink) {
  MESSAGE("constructing a sink");
  format::bro::writer writer{directory};
  auto snk = self->spawn(sink<format::bro::writer>, std::move(writer));
  MESSAGE("sending events");
  self->send(snk, bro_conn_log);
  MESSAGE("shutting down");
  self->send_exit(snk, caf::exit_reason::user_shutdown);
  self->wait_for(snk);
  CHECK(exists(directory / "bro::conn.log"));
}

FIXTURE_SCOPE_END()
