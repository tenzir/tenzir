//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE exporter

#include "vast/system/exporter.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

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
    self->send_exit(importer, caf::exit_reason::user_shutdown);
    self->send_exit(exporter, caf::exit_reason::user_shutdown);
    self->send_exit(index, caf::exit_reason::user_shutdown);
    self->send_exit(meta_index, caf::exit_reason::user_shutdown);
    self->send_exit(archive, caf::exit_reason::user_shutdown);
    run();
  }

  void spawn_type_registry() {
    type_registry
      = self->spawn(system::type_registry, directory / "type-registry");
  }

  void spawn_archive() {
    archive = self->spawn(system::archive, directory / "archive", 1, 1024);
  }

  void spawn_meta_index() {
    meta_index = self->spawn(system::meta_index, system::accountant_actor{});
  }

  void spawn_index() {
    auto fs = self->spawn(system::posix_filesystem, directory);
    auto indexdir = directory / "index";
    index = self->spawn(system::index, system::accountant_actor{}, fs, archive,
                        meta_index, indexdir, defaults::system::store_backend,
                        10000, 5, 5, 1, indexdir, 0.01);
  }

  void spawn_importer() {
    importer
      = self->spawn(system::importer, directory / "importer", archive, index,
                    type_registry, std::vector<vast::transform>{});
  }

  void spawn_exporter(query_options opts) {
    exporter = self->spawn(system::exporter, expr, opts,
                           std::vector<vast::transform>{});
  }

  void importer_setup() {
    if (!type_registry)
      spawn_type_registry();
    if (!archive)
      spawn_archive();
    if (!meta_index)
      spawn_meta_index();
    if (!index)
      spawn_index();
    if (!importer)
      spawn_importer();
    run();
  }

  void exporter_setup(query_options opts) {
    spawn_exporter(opts);
    send(exporter, index);
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
    std::vector<table_slice> result;
    size_t total_events = 0;
    bool running = true;
    self->receive_while(running)(
      [&](table_slice slice) {
        MESSAGE("... got " << slice.rows() << " events");
        total_events += slice.rows();
        result.push_back(std::move(slice));
      },
      error_handler(),
      // Do a one-pass can over the mailbox without waiting for messages.
      caf::after(0ms) >> [&] { running = false; });

    MESSAGE("got " << total_events << " events in total");
    return result;
  }

  void verify(const std::vector<table_slice>& results) {
    auto xs = make_data(results);
    REQUIRE_EQUAL(xs.size(), 5u);
    std::sort(xs.begin(), xs.end());
    CHECK_EQUAL(xs[0][1], "xvWLhxgUmj5");
    CHECK_EQUAL(xs[4][1], "07mJRfg5RU5");
  }

  system::type_registry_actor type_registry;
  system::meta_index_actor meta_index;
  system::index_actor index;
  system::archive_actor archive;
  system::importer_actor importer;
  system::exporter_actor exporter;
  expression expr;
};

} // namespace

FIXTURE_SCOPE(exporter_tests, fixture)

TEST(historical query without importer) {
  MESSAGE("spawn index and archive");
  spawn_archive();
  spawn_meta_index();
  spawn_index();
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
  // The container source copies the zeek_conn_log_slices, so the importer
  // assigning IDs and timestamps to the slices it receives will not mess
  // up our static test data.
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
  send(importer, static_cast<system::stream_sink_actor<table_slice>>(exporter));
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
  send(importer, static_cast<system::stream_sink_actor<table_slice>>(exporter));
  MESSAGE("ingest conn.log via importer");
  // Again: copy because we musn't mutate static test data.
  vast::detail::spawn_container_source(sys, zeek_conn_log, importer);
  run();
  auto results = fetch_results();
  CHECK_EQUAL(rows(results), 0u);
}

FIXTURE_SCOPE_END()
