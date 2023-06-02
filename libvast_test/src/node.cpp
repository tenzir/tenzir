//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "fixtures/node.hpp"

#include "vast/detail/spawn_container_source.hpp"
#include "vast/node.hpp"
#include "vast/query_options.hpp"
#include "vast/query_status.hpp"
#include "vast/uuid.hpp"

using namespace vast;

namespace fixtures {

node::node(std::string_view suite)
  : fixtures::deterministic_actor_system_and_events(suite) {
  MESSAGE("spawning node");
  test_node = self->spawn(vast::node, "test", directory / "node",
                          detach_components::no);
  run();
  auto settings = caf::settings{};
  auto vast_settings = caf::settings{};
  // Don't run the catalog in a separate thread, otherwise it is
  // invisible to the `test_coordinator`.
  caf::put(settings, "vast.detach-components", false);
  // Set the timeout to zero to prevent the index telemetry loop,
  // which will cause any call to `run()` to hang indefinitely.
  caf::put(settings, "vast.active-partition-timeout", caf::timespan{0});
  spawn_component("catalog", {}, settings);
  spawn_component("index", {}, settings);
  spawn_component("importer");
  ingest("zeek");
}

node::~node() {
  self->send_exit(test_node, caf::exit_reason::user_shutdown);
}

void node::ingest(const std::string& type) {
  // Get the importer from the node.
  MESSAGE("getting importer from node");
  caf::actor importer;
  auto rh = self->request(test_node, caf::infinite, atom::get_v, atom::label_v,
                          "importer");
  run();
  rh.receive(
    [&](caf::actor actor) {
      importer = std::move(actor);
    },
    error_handler());
  MESSAGE("sending " << type << " logs");
  // Send previously parsed logs directly to the importer (as opposed to
  // going through a source).
  if (type == "zeek" || type == "all") {
    detail::spawn_container_source(sys, zeek_conn_log, importer);
    // TODO: ship DNS and HTTP log slices when available in the events fixture
    // self->send(importer, zeek_dns_log);
    // self->send(importer, zeek_http_log);
  }
  // TODO: ship slices when available in the events fixture
  // if (type == "bgpdump" || type == "all")
  //   self->send(importer, bgpdump_txt);
  // if (type == "random" || type == "all")
  //   self->send(importer, random);
  run();
  MESSAGE("done ingesting logs");
}

std::vector<table_slice> node::query(std::string expr) {
  MESSAGE("spawn an exporter and register ourselves as sink");
  auto exp
    = spawn_component("exporter", std::vector<std::string>{std::move(expr)});
  self->monitor(exp);
  self->send(exp, atom::sink_v, self);
  self->send(exp, atom::run_v);
  run();
  MESSAGE("fetch results from mailbox");
  std::vector<table_slice> result;
  bool running = true;
  self->receive_while(running)(
    [&](table_slice slice) {
      MESSAGE("... got " << slice.rows() << " events");
      result.push_back(std::move(slice));
    },
    [&](const uuid&, const query_status&) {
      // ignore
    },
    [&](const caf::down_msg& msg) {
      if (msg.reason != caf::exit_reason::normal)
        FAIL("exporter terminated with exit reason: " << to_string(msg.reason));
    },
    // Do a one-pass can over the mailbox without waiting for messages.
    caf::after(std::chrono::seconds(0)) >>
      [&] {
        running = false;
      });
  MESSAGE("got " << result.size() << " table slices in total");
  return result;
}

} // namespace fixtures
