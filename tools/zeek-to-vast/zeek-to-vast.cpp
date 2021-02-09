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

#include "vast/command.hpp"
#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/uuid.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/format/writer.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/connect_to_node.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"
#include "vast/table_slice.hpp"
#include "vast/uuid.hpp"

#include <caf/config_option_adder.hpp>
#include <caf/config_value.hpp>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

#include <broker/broker.hh>
#include <broker/zeek.hh>

using namespace std::chrono_literals;

namespace defaults {

/// The address where the Broker endpoint listens at.
constexpr char broker_address[] = "127.0.0.1";

/// The port where the Broker endpoint binds to.
constexpr uint16_t broker_port = 43000;

/// The address where the VAST node listens at.
constexpr char vast_address[] = "127.0.0.1";

/// The port where the VAST node binds to.
constexpr uint16_t vast_port = 42000;

// The timeout after which a blocking call to retrieve a message from a
// subscriber should return.
broker::duration get_timeout = broker::duration{500ms};

} // namespace defaults

namespace {

constexpr char control_topic[] = "/vast/control";
constexpr char data_topic[] = "/vast/data";

// Global flag that indicates that the application is shutting down due to a
// signal.
std::atomic<bool> terminating = false;

// Double-check the signal handler requirement.
static_assert(decltype(terminating)::is_always_lock_free);

extern "C" void proxy_signal_handler(int sig) {
  // Catch termination signals only once to allow forced termination by the OS
  // upon sending the signal a second time.
  if (sig == SIGINT || sig == SIGTERM)
    std::signal(sig, SIG_DFL);
  terminating = true;
}

// Our custom configuration with extra command line options for this tool.
class config : public broker::configuration {
public:
  config() {
    // Print a reasonable amount of logging output to the console.
    // TODO: switch to console-verbosity after the following PR is merged:
    // https://github.com/actor-framework/actor-framework/pull/766.
    set("logger.verbosity", caf::atom("INFO"));
    set("logger.console", caf::atom("COLORED"));
    // As a stand-alone application, we reuse the global option group from CAF
    // to avoid unnecessary prefixing.
    opt_group{custom_options_, "global"}
      .add<std::string>("vast-address,A",
                        "the address where the Broker endpoints listens")
      .add<uint16_t>("vast-port,P",
                     "the port where the Broker endpoint binds to")
      .add<std::string>("broker-address,a",
                        "the address where the Broker endpoints listens")
      .add<uint16_t>("broker-port,p",
                     "the port where the Broker endpoint binds to")
      .add<bool>("show-progress,s", "print one '.' for each proccessed event");
  }
};

/// Converts VAST data to the corresponding Broker type.
broker::data to_broker(const vast::data& data) {
  return caf::visit(
    vast::detail::overload{
      [](const auto& x) -> broker::data { return x; },
      // TODO: use double-dispatch together with the type to differentiate when
      // we should use broker::port vs. plain counts. Using single dispatch on
      // the data alone is not sufficient to make a proper type conversion.
      [](vast::count x) -> broker::data { return x; },
      [](caf::none_t) -> broker::data { return {}; },
      [](const vast::pattern& x) -> broker::data { return x.string(); },
      [](const vast::address& x) -> broker::data {
        auto bytes
          = reinterpret_cast<const uint32_t*>(std::launder(x.data().data()));
        return broker::address{bytes, broker::address::family::ipv6,
                               broker::address::byte_order::network};
      },
      [](const vast::subnet& x) -> broker::data {
        auto bytes = reinterpret_cast<const uint32_t*>(
          std::launder(x.network().data().data()));
        auto addr = broker::address{bytes, broker::address::family::ipv6,
                                    broker::address::byte_order::network};
        return broker::subnet(addr, x.length());
      },
      [](vast::enumeration x) -> broker::data {
        // FIXME: here we face two different implementation approaches for
        // enums. To represent the actual enum value, Broker uses a string
        // whereas VAST uses a 32-bit unsigned integer. We currently lose
        // the type information by converting the VAST enum into a Broker
        // count. A wholistic approach would include the type information
        // for this data instance and perform the string conversion.
        return broker::count{x};
      },
      [](const vast::list& xs) -> broker::data {
        broker::vector result;
        result.reserve(xs.size());
        std::transform(xs.begin(), xs.end(), std::back_inserter(result),
                       [](const auto& x) { return to_broker(x); });
        return result;
      },
      [](const vast::map& xs) -> broker::data {
        broker::table result;
        auto f = [](const auto& x) {
          return std::pair{to_broker(x.first), to_broker(x.second)};
        };
        std::transform(xs.begin(), xs.end(),
                       std::inserter(result, result.end()), f);
        return result;
      },
      [](const vast::record& xs) -> broker::data {
        broker::vector result;
        result.reserve(xs.size());
        auto f = [](const auto& x) { return to_broker(x.second); };
        std::transform(xs.begin(), xs.end(), std::back_inserter(result), f);
        return result;
      },
    },
    data);
}

// Constructs a result event for Zeek from Broker data.
broker::zeek::Event make_result_event(std::string query_id, broker::data x) {
  broker::vector args(2);
  args[0] = std::move(query_id);
  args[1] = std::move(x);
  return {"VAST::result", std::move(args)};
}

// A VAST writer that publishes the event it gets to a Zeek endpoint.
class zeek_writer : public vast::format::writer {
public:
  zeek_writer() = default;

  zeek_writer(broker::endpoint& endpoint, std::string query_id)
    : endpoint_{&endpoint}, query_id_{std::move(query_id)} {
    auto& cfg = endpoint.system().config();
    show_progress_ = caf::get_or(cfg, "show-progress", false);
  }

  ~zeek_writer() override {
    if (show_progress_ && num_results_ > 0)
      std::cerr << std::endl;
    VAST_INFO("query {} had {} result(s)", query_id_, num_results_);
  }

  using vast::format::writer::write;

  caf::error write(const vast::table_slice& slice) override {
    for (size_t row = 0; row < slice.rows(); ++row) {
      if (show_progress_)
        std::cerr << '.' << std::flush;
      // Assemble an event as a list of broker data values.
      broker::vector xs;
      auto&& layout = slice.layout();
      auto columns = layout.fields.size();
      xs.reserve(columns);
      for (size_t col = 0; col < columns; ++col)
        // TODO: remove unnecessary materialization and operate on data views
        // instead.
        xs.push_back(to_broker(materialize(slice.at(row, col))));
      auto event = make_result_event(query_id_, std::move(xs));
      endpoint_->publish(data_topic, std::move(event));
    }
    num_results_ += slice.rows();
    return caf::none;
  }

  const char* name() const override {
    return "zeek-writer";
  }

private:
  broker::endpoint* endpoint_;
  std::string query_id_;
  bool show_progress_ = false;
  size_t num_results_ = 0;
};

// Parses Broker data as Zeek event.
caf::expected<std::pair<std::string, std::string>>
parse_query_event(const broker::data& x) {
  std::pair<std::string, std::string> result;
  auto event = broker::zeek::Event(x);
  if (event.name() != "VAST::query")
    return caf::make_error(vast::ec::parse_error, "invalid event name",
                           event.name());
  if (event.args().size() != 2)
    return caf::make_error(vast::ec::parse_error, "invalid number of "
                                                  "arguments");
  auto query_id = caf::get_if<std::string>(&event.args()[0]);
  if (!query_id)
    return caf::make_error(vast::ec::parse_error, "invalid type of 1st "
                                                  "argument");
  result.first = *query_id;
  if (!vast::parsers::uuid(*query_id))
    return caf::make_error(vast::ec::parse_error, "invalid query UUID",
                           *query_id);
  auto expression = caf::get_if<std::string>(&event.args()[1]);
  if (!expression)
    return caf::make_error(vast::ec::parse_error, "invalid type of 2nd "
                                                  "argument");
  if (!vast::parsers::expr(*expression))
    return caf::make_error(vast::ec::parse_error, "invalid query expression",
                           *expression);
  result.second = *expression;
  return result;
}

} // namespace <anonymous>

int main(int argc, char** argv) {
  // Parse the command line.
  config cfg;
  vast::detail::add_message_types(cfg);
  cfg.parse(argc, argv);
  if (cfg.cli_helptext_printed)
    return 0;
  std::string broker_address = caf::get_or(cfg, "broker-address",
                                           defaults::broker_address);
  uint16_t broker_port = caf::get_or(cfg, "broker-port", defaults::broker_port);
  // Install signal handler.
  std::signal(SIGINT, proxy_signal_handler);
  std::signal(SIGTERM, proxy_signal_handler);
  // Create a Broker endpoint.
  auto endpoint = broker::endpoint{std::move(cfg)};
  endpoint.listen(broker_address, broker_port);
  // Subscribe to the control channel.
  auto subscriber = endpoint.make_subscriber({control_topic});
  // Connect to VAST via a custom command.
  auto& sys = endpoint.system();
  caf::scoped_actor self{sys};
  std::string vast_address = caf::get_or(sys.config(), "vast-address",
                                         defaults::vast_address);
  uint16_t vast_port = caf::get_or(sys.config(), "vast-port",
                                   defaults::vast_port);
  caf::settings opts;
  // TODO: simplify this to set("global", "endpoint", value) after we addressed
  // https://github.com/actor-framework/actor-framework/issues/769.
  opts.emplace("global",
               caf::config_value::dictionary{{
                 "endpoint", caf::config_value{vast_address + ':'
                                               + std::to_string(vast_port)}}});
  vast::system::node_actor node;
  if (auto conn = vast::system::connect_to_node(self, opts); !conn) {
    VAST_ERROR("failed to connect to VAST: {}", conn.error());
    return 1;
  } else {
    node = std::move(*conn);
  }
  VAST_INFO("connected to VAST successfully");
  // Block until Zeek peers with us.
  auto receive_statuses = true;
  auto status_subscriber = endpoint.make_status_subscriber(receive_statuses);
  auto peered = false;
  while (!peered) {
    auto msg = status_subscriber.get(defaults::get_timeout);
    if (terminating)
      return -1;
    if (!msg)
      continue; // timeout
    caf::visit(vast::detail::overload{
                 [&](broker::none) {
                   // timeout
                 },
                 [&]([[maybe_unused]] broker::error error) {
                   VAST_ERROR("{}", vast::render(error));
                 },
                 [&](broker::status status) {
                   if (status == broker::sc::peer_added)
                     peered = true;
                   else
                     VAST_ERROR("{}", to_string(status));
                 },
               },
               *msg);
  };
  VAST_INFO("peered with Zeek successfully,  waiting for commands");
  // Process queries from Zeek.
  auto done = false;
  while (!done) {
    auto msg = subscriber.get(defaults::get_timeout);
    if (terminating)
      return -1;
    if (!msg)
      continue; // timeout
    auto& [topic, data] = msg->data();
    // Parse the Zeek query event.
    auto result = parse_query_event(data);
    if (!result) {
      VAST_ERROR("{}", vast::render(result.error()));
      continue;
    }
    auto& [query_id, expression] = *result;
    // Relay the query expression to VAST.
    VAST_INFO("dispatching query {} {}", query_id, expression);
    auto inv = vast::invocation{std::move(opts), "", {expression}};
    auto writer = std::make_unique<zeek_writer>(endpoint, query_id);
    auto sink = self->spawn(vast::system::sink, std::move(writer),
                            vast::defaults::export_::max_events);
    vast::scope_linked<caf::actor> guard{sink};
    auto res = vast::system::sink_command(std::move(inv), sys, sink);
    if (res.match_elements<caf::error>()) {
      VAST_ERROR("failed to dispatch query to VAST: {}",
                 res.get_as<caf::error>(0));
      continue;
    }
    // Our Zeek command contains a sink, which terminates automatically when the
    // exporter for the corresponding query has finished. We use this signal to
    // send the final terminator event to Zeek.
    self->monitor(sink);
    self->receive(
      [&, query_id=query_id](const caf::down_msg&) {
        auto nil = broker::data{}; // Avoid ambiguity between VAST & Broker.
        endpoint.publish(data_topic, make_result_event(query_id, nil));
      }
    );
  }
}
