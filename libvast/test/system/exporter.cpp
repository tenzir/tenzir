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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/index.hpp"

#define SUITE export
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

FIXTURE_SCOPE(exporter_tests, fixtures::actor_system_and_events)

TEST(exporter) {
  auto i = self->spawn(system::index, directory / "index", 1000, 5, 5);
  auto a = self->spawn(system::archive, directory / "archive", 1, 1024);
  MESSAGE("ingesting conn.log");
  self->send(i, bro_conn_log);
  self->send(a, bro_conn_log);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing query");
  auto e = self->spawn(system::exporter, *expr, historical);
  self->send(e, a);
  self->send(e, system::index_atom::value, i);
  self->send(e, system::sink_atom::value, self);
  self->send(e, system::run_atom::value);
  self->send(e, system::extract_atom::value);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(i, exit_reason::user_shutdown);
  self->send_exit(a, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
