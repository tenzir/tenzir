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

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/filesystem.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
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
    for (auto& hdl : {index, importer, exporter})
      self->send_exit(hdl, exit_reason::user_shutdown);
    self->send_exit(archive, exit_reason::user_shutdown);
    run();
  }

  void spawn_type_registry() {
    type_registry
      = self->spawn(system::type_registry, directory / "type-registry");
  }

  void spawn_index() {
    auto fs = self->spawn(system::posix_filesystem, directory);
    index = self->spawn(system::index, fs, directory / "index", 10000, 5, 5, 1, true);
  }

  void spawn_archive() {
    archive = self->spawn(system::archive, directory / "archive", 1, 1024);
  }

  void spawn_importer() {
    importer = self->spawn(system::importer, directory / "importer", archive,
                           index, type_registry);
  }

  void spawn_exporter(query_options opts) {
    exporter = self->spawn(system::exporter, expr, opts);
  }

  void importer_setup() {
    if (!type_registry)
      spawn_type_registry();
    if (!index)
      spawn_index();
    if (!archive)
      spawn_archive();
    if (!importer)
      spawn_importer();
    run();
  }

  void exporter_setup(query_options opts) {
    spawn_exporter(opts);
    send(exporter, archive);
    send(exporter, atom::index_v, index);
    send(exporter, atom::sink_v, self);
    send(exporter, atom::run_v);
    send(exporter, atom::extract_v);
    run();
  }

  template <class Hdl, class... Ts>
  void send(Hdl hdl, Ts&&... xs) {
    self->send(hdl, std::forward<Ts>(xs)...);
  }

  auto fetch_results() {
    MESSAGE("fetching results");
    std::vector<table_slice_ptr> result;
    size_t total_events = 0;
    bool running = true;
    self->receive_while(running)(
      [&](table_slice_ptr slice) {
        MESSAGE("... got " << slice->rows() << " events");
        total_events += slice->rows();
        result.push_back(std::move(slice));
      },
      error_handler(),
      // Do a one-pass can over the mailbox without waiting for messages.
      after(0ms) >> [&] { running = false; });

    MESSAGE("got " << total_events << " events in total");
    return result;
  }

  void verify(const std::vector<table_slice_ptr>& results) {
    auto xs = to_data(results);
    REQUIRE_EQUAL(xs.size(), 5u);
    std::sort(xs.begin(), xs.end());
    CHECK_EQUAL(xs[0][1], "xvWLhxgUmj5");
    CHECK_EQUAL(xs[4][1], "07mJRfg5RU5");
  }

  system::type_registry_type type_registry;
  actor index;
  system::archive_type archive;
  actor importer;
  actor exporter;
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
  vast::detail::spawn_container_source(sys, zeek_conn_log, index, archive);
  run();
  MESSAGE("spawn exporter for historical query");
  exporter_setup(historical);
  verify(fetch_results());
}

TEST(historical query with importer) {
  MESSAGE("prepare importer");
  importer_setup();
  MESSAGE("ingest conn.log via importer");
  // We need to copy zeek_conn_log_slices here, because the importer will assign
  // IDs to the slices it received and we mustn't mess our static test data.
  vast::detail::spawn_container_source(sys, zeek_conn_log, importer);
  run();
  MESSAGE("spawn exporter for historical query");
  exporter_setup(historical);
  verify(fetch_results());
}

TEST(continuous query with exporter only) {
  MESSAGE("prepare exporter for continuous query");
  spawn_exporter(continuous);
  send(exporter, atom::sink_v, self);
  send(exporter, atom::extract_v);
  run();
  MESSAGE("send conn.log directly to exporter");
  vast::detail::spawn_container_source(sys, zeek_conn_log, exporter);
  run();
  verify(fetch_results());
}

TEST(continuous query with importer) {
  MESSAGE("prepare importer");
  importer_setup();
  MESSAGE("prepare exporter for continous query");
  exporter_setup(continuous);
  send(importer, atom::exporter_v, exporter);
  MESSAGE("ingest conn.log via importer");
  // Again: copy because we musn't mutate static test data.
  vast::detail::spawn_container_source(sys, zeek_conn_log, importer);
  run();
  verify(fetch_results());
}

TEST(continuous query with mismatching importer) {
  MESSAGE("prepare importer");
  importer_setup();
  MESSAGE("prepare exporter for continous query");
  expr = unbox(to<expression>("foo.bar == \"baz\""));
  exporter_setup(continuous);
  send(importer, atom::exporter_v, exporter);
  MESSAGE("ingest conn.log via importer");
  // Again: copy because we musn't mutate static test data.
  vast::detail::spawn_container_source(sys, zeek_conn_log, importer);
  run();
  auto results = fetch_results();
  CHECK_EQUAL(rows(results), 0u);
}

FIXTURE_SCOPE_END()
