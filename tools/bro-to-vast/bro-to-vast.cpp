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

// TODO:
// - Handle errors/statuses asynchronously
// - Add mode to use streaming API
// - Connect to VAST

#include <cstdint>
#include <iostream>
#include <string>

#include <caf/config_option_adder.hpp>

#include <broker/bro.hh>
#include <broker/broker.hh>

#include <vast/error.hpp>
#include <vast/event.hpp>
#include <vast/expression.hpp>
#include <vast/uuid.hpp>

#include <vast/concept/parseable/parse.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/uuid.hpp>

#include <vast/detail/assert.hpp>
#include <vast/detail/overload.hpp>

namespace {

constexpr char default_control_topic[] = "/vast/control";
constexpr char default_data_topic[] = "/vast/data";
constexpr char default_address[] = "localhost";
constexpr uint16_t default_port = 43000;

// Our custom configuration with extra command line options for this tool.
class config : public broker::configuration {
public:
  config() {
    // As a stand-alone application, we reuse the global option group from CAF
    // to avoid unnecessary prefixing.
    opt_group{custom_options_, "global"}
      .add<std::string>("control-topic,c",
                        "the topic for exchanging control messages")
      .add<std::string>("data-topic,d",
                        "the topic for exchanging data messages")
      .add<uint16_t>("port,p", "the port to listen at or connect to");
  }
};

// Performs a lookup of a VAST query expression.
caf::expected<std::vector<broker::data>> lookup(const std::string& expr) {
  std::cerr << "answering query '" << expr << "'" << std::endl;
  std::vector<broker::data> result;
  // TODO: get actual results from VAST.
  for (auto i : {42, 43, 44, 45, 46})
    result.emplace_back(broker::vector{i});
  return result;
}

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
  std::string control_topic = caf::get_or<std::string>(cfg, "control-topic",
                                                       default_control_topic);
  std::string data_topic = caf::get_or<std::string>(cfg, "data-topic",
                                                    default_data_topic);
  // Create a Broker endpoint.
  auto endpoint = broker::endpoint{std::move(cfg)};
  endpoint.listen(address, port);
  // Subscribe to the control channel.
  auto subscriber = endpoint.make_subscriber({control_topic});
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
        std::cerr << to_string(error) << std::endl;
      },
      [&](broker::status status) {
        if (status == broker::sc::peer_added)
          peered = true;
        else
          std::cerr << to_string(status) << std::endl;
      }
    ), msg);
  };
  std::cerr << "established peering successfully" << std::endl;
  // Process queries from Bro.
  auto done = false;
  while (!done) {
    std::cerr << "waiting for commands" << std::endl;
    auto [topic, data] = subscriber.get();
    // Parse the Bro query event.
    auto result = parse_query_event(data);
    if (!result) {
      std::cerr << to_string(result.error()) << std::endl;
      return 1;
    }
    auto& [query_id, expression] = *result;
    // Relay the query expression to VAST.
    auto xs = lookup(expression);
    if (!xs) {
      std::cerr << to_string(xs.error()) << std::endl;
      return 1;
    }
    // Send results back to Bro. A none value signals that the query has
    // completed.
    auto make_result_event = [uuid=query_id](auto x) {
      broker::vector args(2);
      args[0] = std::move(uuid);
      args[1] = std::move(x);
      return broker::bro::Event{"VAST::result", std::move(args)};
    };
    for (auto& x : *xs)
      endpoint.publish(data_topic, make_result_event(std::move(x)));
    endpoint.publish(data_topic, make_result_event(broker::nil));
  }
}
