#ifndef FIXTURES_NODE_HPP
#define FIXTURES_NODE_HPP

#include <caf/all.hpp>

#include "vast/query_options.hpp"
#include "vast/uuid.hpp"

#include "vast/system/node.hpp"
#include "vast/system/query_statistics.hpp"

#include "data.hpp"
#include "fixtures/actor_system_and_events.hpp"

namespace fixtures {

using namespace vast;

struct node : actor_system_and_events {
  node() {
    test_node = self->spawn(system::node, "test", directory / "node");
    MESSAGE("spawning components");
    spawn_component("metastore");
    spawn_component("archive");
    spawn_component("index");
    spawn_component("importer");
  }

  ~node() {
    self->send_exit(test_node, caf::exit_reason::user_shutdown);
    self->wait_for(test_node);
  }

  template <class... Ts>
  caf::actor spawn_component(std::string component, Ts&&... args) {
    using namespace caf;
    actor result;
    auto cmd_args = make_message(std::move(component),
                                 std::forward<Ts>(args)...);
    self->request(test_node, infinite, "spawn", std::move(cmd_args)).receive(
      [&](const actor& a) { result = a; },
      [&](const error& e) {
        FAIL("failed to spawn " << component << ": " << system.render(e));
       }
    );
    return result;
  }

  // Ingests a specific type of logs.
  void ingest(const std::string& type) {
    using namespace caf;
    // Get the importer from the node.
    MESSAGE("getting importer from node");
    actor importer;
    self->request(test_node, infinite, get_atom::value).receive(
      [&](const std::string& id, system::registry& reg) {
        auto er = reg.components[id].equal_range("importer");
        if (er.first == er.second)
          FAIL("no importers available at test node");
        importer = er.first->second.actor;
      },
      error_handler()
    );
    MESSAGE("sending " << type << " logs");
    // Send previously parsed logs directly to the importer (as opposed to
    // going through a source).
    if (type == "bro" || type == "all") {
      self->send(importer, bro_conn_log);
      self->send(importer, bro_dns_log);
      self->send(importer, bro_http_log);
    }
    if (type == "bgpdump" || type == "all")
      self->send(importer, bgpdump_txt);
    if (type == "random" || type == "all")
      self->send(importer, random);
  }

  // Performs a historical query and returns the resulting events.
  std::vector<event> query(std::string expr) {
    // Spawn an exporter and register ourselves as sink.
    auto exp = spawn_component("exporter", std::move(expr));
    self->monitor(exp);
    self->send(exp, system::sink_atom::value, self);
    self->send(exp, system::run_atom::value);
    self->send(exp, system::extract_atom::value);
    std::vector<event> result;
    auto done = false;
    self->do_receive(
      [&](std::vector<event>& xs) {
        result.insert(result.end(), std::make_move_iterator(xs.begin()),
                      std::make_move_iterator(xs.end()));
      },
      [&](const uuid&, const system::query_statistics&) {
        // ignore
      },
      [&](const caf::down_msg& msg) {
        if (msg.reason != caf::exit_reason::normal)
          FAIL("terminated with exit reason: " << to_string(msg.reason));
        done = true;
      }
    ).until([&]{ return done; });
    return result;
  }

  caf::actor test_node;
};

} // namespace fixtures

#endif
