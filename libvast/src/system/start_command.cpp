//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/start_command.hpp"

#include "vast/fwd.hpp"

#include "vast/command.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/settings.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/application.hpp"
#include "vast/system/connect_to_node.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/systemd.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>

namespace vast::system {

namespace {

/// Actor to run one of the additional commands given as
/// parameters to the VAST node.
using command_runner_actor = system::typed_actor_fwd<
  // Handle a request.
  auto(atom::run, invocation)->caf::result<void>>::unwrap;

command_runner_actor::behavior_type
command_runner(command_runner_actor::pointer self) {
  return {
    [self](atom::run, vast::invocation& invocation) -> caf::result<void> {
      auto [root, root_factory] = make_application("vast");
      auto result = run(invocation, self->home_system(), root_factory);
      if (!result)
        VAST_ERROR("failed to run start command {}: {}", invocation,
                   result.error());
      return {};
    },
  };
}

} // namespace

using namespace std::chrono_literals;

caf::message start_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG(inv.options),
                   VAST_ARG("args", inv.arguments.begin(),
                            inv.arguments.end()));
  // Bail out early for bogus invocations.
  if (caf::get_or(inv.options, "vast.node", false))
    return caf::make_message(
      caf::make_error(ec::invalid_configuration,
                      "unable to run 'vast start' when spawning a "
                      "node locally instead of connecting to one; please "
                      "unset the option vast.node"));
  // Construct an endpoint.
  endpoint node_endpoint;
  auto str
    = get_or(inv.options, "vast.endpoint", defaults::system::endpoint.data());
  if (!parsers::endpoint(str, node_endpoint))
    return caf::make_message(
      caf::make_error(ec::parse_error, "invalid endpoint", str));
  // Default to port 42000/tcp if none is set.
  if (!node_endpoint.port)
    node_endpoint.port = port{defaults::system::endpoint_port, port_type::tcp};
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Spawn our node.
  auto node_opt = spawn_node(self, content(sys.config()));
  if (!node_opt)
    return caf::make_message(std::move(node_opt.error()));
  auto const& node = node_opt->get();
  // FIXME: Encapsulate this logic into a `fleet-client` plugin
  if (!sys.has_openssl_manager()) {
    return caf::make_message(caf::make_error(
      ec::invalid_argument, "need valid tls credentials to connect to fleet"));
  }
  auto is_fleet_manager
    = caf::get_or(inv.options, "vast.fleet.is-manager-node", false);
  // Publish our node.
  auto const* host = node_endpoint.host.empty()
                       ? defaults::system::endpoint_host.data()
                       : node_endpoint.host.c_str();
  auto publish = [&]() -> caf::expected<uint16_t> {
    const auto reuse_address = true;
    // The manager node only and always requires TLS connections, the
    // non-manager nodes always publish the non-TLS endpoints (so that
    // client commands can connect normally) and use TLS only when
    // connecting to the manager.
    if (is_fleet_manager) {
      return caf::openssl::publish(node, node_endpoint.port->number(), host,
                                   reuse_address);
    }
    auto& mm = sys.middleman();
    return mm.publish(node, node_endpoint.port->number(), host, reuse_address);
  };
  auto bound_port = publish();
  if (!bound_port)
    return caf::make_message(std::move(bound_port.error()));
  auto listen_addr = std::string{host} + ':' + std::to_string(*bound_port);
  VAST_INFO("VAST ({}) is listening on {}", version::version, listen_addr);
  // Notify the service manager if it expects an update.
  if (auto error = systemd::notify_ready())
    return caf::make_message(std::move(error));
  // Run main loop.
  caf::error err;
  auto stop = false;
  self->monitor(node);
  // A single line of output to publish out address for scripts.
  if (caf::get_or(inv.options, "vast.start.print-endpoint", false))
    std::cout << listen_addr << std::endl;
  auto commands = caf::get_or(inv.options, "vast.start.commands",
                              std::vector<std::string>{});
  if (commands.empty()) {
    if (auto const* command
        = caf::get_if<std::string>(&inv.options, "vast.start.commands"))
      commands.push_back(*command);
  }
  // ---
  auto node2 = caf::expected<node_actor>{caf::none};
  if (!is_fleet_manager) {
    auto const* manager_url
      = caf::get_if<std::string>(&inv.options, "vast.fleet.manager-url");
    if (manager_url == nullptr)
      return caf::make_message(
        caf::make_error(ec::invalid_argument, "missing manager url"));
    auto node2_opts = caf::settings{};
    caf::put(node2_opts, "vast.endpoint", *manager_url);
    node2 = connect_to_node(self, node2_opts);
    if (node2)
      VAST_INFO("connected to node {}", *node2);
  }
  // ---
  std::vector<command_runner_actor> command_runners;
  if (!commands.empty()) {
    auto [root, root_factory] = make_application("vast");
    // We're already in the start command, so we can safely assert that
    // make_application works as expected.
    VAST_ASSERT(root);
    for (auto const& command : commands) {
      // We use std::quoted for correct tokenization of quoted strings. The
      // invocation parser expects a vector of strings that are correctly
      // tokenized already.
      auto tokenizer = std::stringstream{command};
      auto cli = std::vector<std::string>{};
      auto current = std::string{};
      while (tokenizer >> std::quoted(current))
        cli.push_back(std::move(current));
      VAST_INFO("running post-start command {}", command);
      auto hook_invocation = parse(*root, cli.begin(), cli.end());
      if (!hook_invocation)
        return caf::make_message(hook_invocation.error());
      detail::merge_settings(inv.options, hook_invocation->options,
                             policy::merge_lists::yes);
      auto runner = self->spawn<caf::detached>(command_runner);
      command_runners.push_back(runner);
      self->send(runner, atom::run_v, *hook_invocation);
    }
  }
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        VAST_ASSERT(msg.source == node);
        VAST_DEBUG("{} received DOWN from node", *self);
        stop = true;
        if (msg.reason != caf::exit_reason::user_shutdown)
          err = std::move(msg.reason);
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", *self, ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
        self->send_exit(node, caf::exit_reason::user_shutdown);
      })
    .until([&] {
      return stop;
    });
  return caf::make_message(std::move(err));
}

} // namespace vast::system
