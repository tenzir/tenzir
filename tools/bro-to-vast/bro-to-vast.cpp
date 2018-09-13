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

#include <cstdint>
#include <iostream>
#include <string>

#include <caf/config_option_adder.hpp>

#include <broker/bro.hh>
#include <broker/broker.hh>

#include <vast/defaults.hpp>
#include <vast/error.hpp>
#include <vast/event.hpp>
#include <vast/expression.hpp>
#include <vast/uuid.hpp>

#include <vast/concept/parseable/parse.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/uuid.hpp>

#include <vast/system/writer_command_base.hpp>
#include <vast/system/sink.hpp>

#include <vast/detail/assert.hpp>
#include <vast/detail/overload.hpp>

namespace {

constexpr char control_topic[] = "/vast/control";
constexpr char data_topic[] = "/vast/data";

constexpr char default_address[] = "localhost";
constexpr uint16_t default_port = 43000;

// Our custom configuration with extra command line options for this tool.
class config : public broker::configuration {
public:
  config() {
    // As a stand-alone application, we reuse the global option group from CAF
    // to avoid unnecessary prefixing.
    opt_group{custom_options_, "global"}
      .add<uint16_t>("port,p", "the port to listen at or connect to");
  }
};

// Constructs a result event for Bro.
broker::bro::Event make_bro_event(std::string id, broker::data x) {
  broker::vector args(2);
  args[0] = std::move(id);
  args[1] = std::move(x);
  return {"VAST::result", std::move(args)};
}

// A VAST writer that publishes the event it gets to a Bro endpoint.
class bro_writer {
public:
  bro_writer() = default;

  bro_writer(broker::endpoint& endpoint, std::string query_id)
    : endpoint_{&endpoint},
      query_id_{std::move(query_id)} {
    // nop
  }

  caf::expected<void> write(const vast::event& x) {
    // TODO: publish to Broker endpoint.
    std::cout << x.type().name() << std::endl;
    return caf::no_error;
  }

  caf::expected<void> flush() {
    return caf::no_error;
  }

  auto name() const {
    return "bro-writer";
  }

private:
  broker::endpoint* endpoint_;
  std::string query_id_;
};

// A custom command that allows us to re-use VAST command dispatching logic in
// order to issue a query that writes into a sink with a custom format.
class bro_command : public vast::system::writer_command_base {
public:
  bro_command(broker::endpoint& endpoint)
    : writer_command_base{nullptr, "bro"},
      endpoint_{endpoint} {
    // nop
  }

  // Sets the query ID to the UUID provided by Bro.
  void query_id(std::string id) {
    query_id_ = std::move(id);
  }

protected:
  caf::expected<caf::actor> make_sink(caf::scoped_actor& self,
                                      const caf::config_value_map& options,
                                      argument_iterator begin,
                                      argument_iterator end) override {
    VAST_UNUSED(options, begin, end);
    bro_writer writer{endpoint_, query_id_};
    return self->spawn(vast::system::sink<bro_writer>, std::move(writer),
                       vast::defaults::command::max_events);
  }

private:
  broker::endpoint& endpoint_;
  std::string query_id_;
};


// Parses Broker data as Bro event.
caf::expected<std::pair<std::string, std::string>>
parse_query_event(const broker::data& x) {
  std::pair<std::string, std::string> result;
  auto event = broker::bro::Event(x);
  if (event.name() != "VAST::query")
    return make_error(vast::ec::parse_error, "invalid event name", event.name());
  if (event.args().size() != 2)
    return make_error(vast::ec::parse_error, "invalid number of arguments");
  auto query_id = caf::get_if<std::string>(&event.args()[0]);
  if (!query_id)
    return make_error(vast::ec::parse_error, "invalid type of 1st argument");
  result.first = *query_id;
  if (!vast::parsers::uuid(*query_id))
    return make_error(vast::ec::parse_error, "invalid query UUID", *query_id);
  auto expression = caf::get_if<std::string>(&event.args()[1]);
  if (!expression)
    return make_error(vast::ec::parse_error, "invalid type of 2nd argument");
  if (!vast::parsers::expr(*expression))
    return make_error(vast::ec::parse_error, "invalid query expression",
                      *expression);
  result.second = *expression;
  return result;
}

} // namespace <anonymous>

int main(int argc, char** argv) {
  // Parse the command line.
  config cfg;
  cfg.parse(argc, argv);
  std::string address = caf::get_or(cfg, "address", default_address);
  uint16_t port = caf::get_or(cfg, "port", default_port);
  // Create a Broker endpoint.
  auto endpoint = broker::endpoint{std::move(cfg)};
  endpoint.listen(address, port);
  // Subscribe to the control channel.
  auto subscriber = endpoint.make_subscriber({control_topic});
  // Connect to VAST via a custom command.
  bro_command cmd{endpoint};
  auto& sys = endpoint.system();
  caf::scoped_actor self{sys};
  caf::config_value_map opts;
  auto node = cmd.connect_to_node(self, opts);
  if (!node) {
    std::cerr << "failed to connect to VAST: " << sys.render(node.error())
              << std::endl;
    return 1;
  }
  std::cerr << "connected to VAST successfully" << std::endl;
  // Block until Bro peers with us.
  auto receive_statuses = true;
  auto status_subscriber = endpoint.make_status_subscriber(receive_statuses);
  auto peered = false;
  while (!peered) {
    auto msg = status_subscriber.get();
    caf::visit(vast::detail::overload(
      [&](broker::none) {
        // timeout
      },
      [&](broker::error error) {
        std::cerr << sys.render(error) << std::endl;
      },
      [&](broker::status status) {
        if (status == broker::sc::peer_added)
          peered = true;
        else
          std::cerr << to_string(status) << std::endl;
      }
    ), msg);
  };
  std::cerr << "peered with Bro successfully" << std::endl;
  // Process queries from Bro.
  auto done = false;
  while (!done) {
    std::cerr << "waiting for commands" << std::endl;
    auto [topic, data] = subscriber.get();
    // Parse the Bro query event.
    auto result = parse_query_event(data);
    if (!result) {
      std::cerr << sys.render(result.error()) << std::endl;
      return 1;
    }
    auto& [query_id, expression] = *result;
    // Relay the query expression to VAST.
    cmd.query_id(query_id);
    auto args = std::vector<std::string>{expression};
    auto rc = cmd.run(sys, args.begin(), args.end());
    if (rc != 0) {
      std::cerr << "failed to dispatch query to VAST" << std::endl;
      return rc;
    }
    // A none value signals that the query has completed.
    endpoint.publish(data_topic, make_bro_event(query_id, broker::nil));
  }
}
