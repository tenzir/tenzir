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

#define SUITE exporter

#include "vast/system/exporter.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/replicated_store.hpp"
#include "vast/table_slice.hpp"

using namespace caf;
using namespace vast;

using std::string;
using std::chrono_literals::operator""ms;

namespace {

using fixture_base = fixtures::deterministic_actor_system_and_events;

struct fixture : fixture_base {
  fixture() {
    expr = unbox(to<expression>("service == \"dns\" "
                                "&& :addr == 192.168.1.1"));
  }

  ~fixture() {
    for (auto& hdl : {index, importer, exporter, raft_consensus})
      self->send_exit(hdl, exit_reason::user_shutdown);
    self->send_exit(archive, exit_reason::user_shutdown);
    self->send_exit(consensus, exit_reason::user_shutdown);
    run();
  }

  void spawn_index() {
    index = self->spawn(system::index, directory / "index", 10000, 5, 5, 1);
  }

  void spawn_archive() {
    archive = self->spawn(system::archive, directory / "archive", 1, 1024);
  }

  void spawn_importer() {
    importer = self->spawn(system::importer, directory / "importer",
                           slice_size);
  }

  void spawn_raft_consensus() {
    raft_consensus = self->spawn(system::raft::consensus, directory / "consensus");
  }

  void spawn_consensus() {
    if (!raft_consensus)
      spawn_raft_consensus();
    consensus = self->spawn(system::replicated_store<string, data>, raft_consensus);
  }

  void spawn_exporter(query_options opts) {
    exporter = self->spawn(system::exporter, expr, opts);
  }

  void importer_setup() {
    if (!index)
      spawn_index();
    if (!archive)
      spawn_archive();
    if (!importer)
      spawn_importer();
    if (!consensus)
      spawn_consensus();
    send(raft_consensus, system::run_atom::value);
    run();
    send(importer, archive);
    send(importer, system::index_atom::value, index);
    send(importer, consensus);
    run();
  }

  void exporter_setup(query_options opts) {
    spawn_exporter(opts);
    send(exporter, archive);
    send(exporter, system::index_atom::value, index);
    send(exporter, system::sink_atom::value, self);
    send(exporter, system::run_atom::value);
    send(exporter, system::extract_atom::value);
    run();
  }

  template <class Hdl, class... Ts>
  void send(Hdl hdl, Ts&&... xs) {
    self->send(hdl, std::forward<Ts>(xs)...);
  }

  auto fetch_results() {
    std::vector<event> result;
    bool done = false;
    self->do_receive(
      [&](std::vector<event>& xs) {
        MESSAGE("... got " << xs.size() << " events");
        std::move(xs.begin(), xs.end(), std::back_inserter(result));
      },
      error_handler(),
      after(0ms) >> [&] {
        done = true;
      }
    ).until(done);
    MESSAGE("got " << result.size() << " events in total");
    return result;
  }

  actor index;
  system::archive_type archive;
  actor importer;
  actor exporter;
  actor raft_consensus;
  system::consensus_type consensus;
  expression expr;
};

} // namespace <anonymous>

FIXTURE_SCOPE(exporter_tests, fixture)

TEST(historical query without importer) {
  MESSAGE("spawn index and archive");
  spawn_index();
  spawn_archive();
  run();
  MESSAGE("ingest conn.log into archive and index");
  vast::detail::spawn_container_source(sys, zeek_conn_log_slices, index,
                                       archive);
  run();
  MESSAGE("spawn exporter for historical query");
  exporter_setup(historical);
  MESSAGE("fetch results");
  auto results = fetch_results();
  REQUIRE_EQUAL(results.size(), 5u);
  std::sort(results.begin(), results.end());
  CHECK_EQUAL(results.front().id(), 10u);
  CHECK_EQUAL(results.front().type().name(), "zeek.conn");
  CHECK_EQUAL(results.back().id(), 19u);
}

TEST(historical query with importer) {
  MESSAGE("prepare importer");
  importer_setup();
  MESSAGE("ingest conn.log via importer");
  // We need to copy zeek_conn_log_slices here, because the importer will assign
  // IDs to the slices it received and we mustn't mess our static test data.
  vast::detail::spawn_container_source(sys, copy(zeek_conn_log_slices),
                                       importer);
  run();
  MESSAGE("spawn exporter for historical query");
  exporter_setup(historical);
  MESSAGE("fetch results");
  auto results = fetch_results();
  REQUIRE_EQUAL(results.size(), 5u);
  std::sort(results.begin(), results.end());
  CHECK_EQUAL(results.front().id(), 10u);
  CHECK_EQUAL(results.front().type().name(), "zeek.conn");
  CHECK_EQUAL(results.back().id(), 19u);
}

TEST(continuous query with exporter only) {
  MESSAGE("prepare exporter for continuous query");
  spawn_exporter(continuous);
  send(exporter, system::sink_atom::value, self);
  send(exporter, system::extract_atom::value);
  run();
  MESSAGE("send conn.log directly to exporter");
  vast::detail::spawn_container_source(sys, zeek_conn_log_slices, exporter);
  run();
  MESSAGE("fetch results");
  auto results = fetch_results();
  REQUIRE_EQUAL(results.size(), 5u);
  std::sort(results.begin(), results.end());
  CHECK_EQUAL(results.front().id(), 10u);
  CHECK_EQUAL(results.front().type().name(), "zeek.conn");
  CHECK_EQUAL(results.back().id(), 19u);
}

TEST(continuous query with importer) {
  MESSAGE("prepare importer");
  importer_setup();
  MESSAGE("prepare exporter for continous query");
  exporter_setup(continuous);
  send(importer, system::exporter_atom::value, exporter);
  MESSAGE("ingest conn.log via importer");
  // Again: copy because we musn't mutate static test data.
  vast::detail::spawn_container_source(sys, copy(zeek_conn_log_slices),
                                       importer);
  run();
  MESSAGE("fetch results");
  auto results = fetch_results();
  REQUIRE_EQUAL(results.size(), 5u);
  std::sort(results.begin(), results.end());
  CHECK_EQUAL(results.front().id(), 10u);
  CHECK_EQUAL(results.front().type().name(), "zeek.conn");
  CHECK_EQUAL(results.back().id(), 19u);
}

FIXTURE_SCOPE_END()
