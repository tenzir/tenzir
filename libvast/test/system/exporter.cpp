//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/exporter.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/query_options.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include <caf/attach_stream_sink.hpp>

using namespace vast;

using std::string;
using std::chrono_literals::operator""ms;

namespace {

using fixture_base = fixtures::deterministic_actor_system_and_events;

caf::behavior
dummy_sink(caf::event_based_actor* self, std::vector<table_slice>* result) {
  return {
    [=](caf::stream<table_slice> in) {
      caf::attach_stream_sink(
        self, in,
        [](caf::unit_t&) {
          // nop
        },
        [result](caf::unit_t&, table_slice&& x) {
          result->push_back(x);
        })
        .inbound_slot();
    },
  };
}

struct fixture : fixture_base {
  fixture() : fixture_base(VAST_PP_STRINGIFY(SUITE)) {
    expr = unbox(to<expression>("service == \"dns\" "
                                "&& :ip == 192.168.1.1"));
  }

  ~fixture() override {
    self->send_exit(sink, caf::exit_reason::user_shutdown);
    self->send_exit(importer, caf::exit_reason::user_shutdown);
    self->send_exit(exporter, caf::exit_reason::user_shutdown);
    self->send_exit(index, caf::exit_reason::user_shutdown);
    self->send_exit(catalog, caf::exit_reason::user_shutdown);
    run();
  }

  void spawn_catalog() {
    catalog = self->spawn(system::catalog, system::accountant_actor{},
                          directory / "type-registry");
  }

  void spawn_index() {
    auto fs = self->spawn(system::posix_filesystem, directory,
                          system::accountant_actor{});
    auto indexdir = directory / "index";
    index = self->spawn(system::index, system::accountant_actor{}, fs, catalog,
                        indexdir, defaults::system::store_backend, 10000,
                        duration{}, 5, 5, 1, indexdir, vast::index_config{});
  }

  void spawn_importer() {
    importer
      = self->spawn(system::importer, directory / "importer", index,
                    system::accountant_actor{}, std::vector<vast::pipeline>{});
  }

  void spawn_exporter(query_options opts) {
    exporter = self->spawn(system::exporter, expr, opts,
                           std::vector<vast::pipeline>{}, index);
  }

  void spawn_sink() {
    sink = self->spawn(dummy_sink, std::addressof(sink_received_slices));
  }

  void importer_setup() {
    if (!catalog)
      spawn_catalog();
    if (!index)
      spawn_index();
    if (!importer)
      spawn_importer();
  }

  void exporter_setup(query_options opts) {
    spawn_exporter(opts);
    spawn_sink();
    send(exporter, atom::sink_v, sink);
    send(exporter, atom::run_v);
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
      caf::after(0ms) >>
        [&] {
          running = false;
        });

    MESSAGE("got " << total_events << " events in total");
    return sink_received_slices;
  }

  void verify(const std::vector<table_slice>& results) {
    auto xs = make_data(results);
    REQUIRE_EQUAL(xs.size(), 5u);
    std::sort(xs.begin(), xs.end());
    CHECK_EQUAL(xs[0][1], "xvWLhxgUmj5");
    CHECK_EQUAL(xs[4][1], "07mJRfg5RU5");
  }

  system::catalog_actor catalog;
  system::index_actor index;
  system::importer_actor importer;
  system::exporter_actor exporter;
  caf::actor sink;
  expression expr;
  std::vector<table_slice> sink_received_slices;
};

} // namespace

FIXTURE_SCOPE(exporter_tests, fixture)

TEST(historical query without importer) {
  MESSAGE("spawn index");
  spawn_catalog();
  spawn_index();
  run();
  MESSAGE("ingest conn.log into index");
  vast::detail::spawn_container_source(sys, zeek_conn_log, index);
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
  spawn_catalog();
  spawn_index();
  run();
  spawn_exporter(continuous);
  spawn_sink();
  send(exporter, atom::sink_v, sink);
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
