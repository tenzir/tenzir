//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/start_command.hpp"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/settings.hpp"
#include "vast/system/application.hpp"
#include "vast/systemd.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/io/middleman.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <csignal>
#include <thread>
#if VAST_ENABLE_OPENSSL
#  include <caf/openssl/all.hpp>
#endif // VAST_ENABLE_OPENSSL

#include "vast/fwd.hpp"

#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/defaults.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_node.hpp"

namespace vast::system {

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
  auto str = get_or(inv.options, "vast.endpoint", defaults::system::endpoint);
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
  auto& node = node_opt->get();
  // Publish our node.
  auto host = node_endpoint.host.empty()
                ? defaults::system::endpoint_host.data()
                : node_endpoint.host.c_str();
  auto publish = [&]() -> caf::expected<uint16_t> {
    const auto reuse_address = true;
    if (sys.has_openssl_manager()) {
#if VAST_ENABLE_OPENSSL
      return caf::openssl::publish(node, node_endpoint.port->number(), host,
                                   reuse_address);
#else
      return caf::make_error(ec::unspecified, "not compiled with OpenSSL "
                                              "support");
#endif
    }
    auto& mm = sys.middleman();
    return mm.publish(node, node_endpoint.port->number(), host, reuse_address);
  };
  auto bound_port = publish();
  if (!bound_port)
    return caf::make_message(std::move(bound_port.error()));
  auto listen_addr = std::string{host} + ':' + std::to_string(*bound_port);
  VAST_INFO("VAST ({}) is listening on {}", version::version, listen_addr);
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
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
    if (auto command = caf::get_if<std::string>( //
          &inv.options, "vast.start.commands"))
      commands.push_back(std::move(*command));
  }
  if (!commands.empty()) {
    auto [root, root_factory] = make_application("vast");
    // We're already in the start command, so we can safely assert that
    // make_application works as expected.
    VAST_ASSERT(root);
    for (const auto& command : commands) {
      // We use std::quoted for correct tokenization of quoted strings. The
      // invocation parser expects a vector of strings that are correctly
      // tokenized already.
      auto tokenizer = std::stringstream{command};
      auto cli = std::vector<std::string>{};
      auto current = std::string{};
      while (tokenizer >> std::quoted(current))
        cli.push_back(std::move(current));
      VAST_INFO("running post-start command {}", command);
      auto hook_invocation = parse(*root, cli);
      if (!hook_invocation)
        return caf::make_message(hook_invocation.error());
      detail::merge_settings(inv.options, hook_invocation->options,
                             policy::merge_lists::yes);
      auto result = run(*hook_invocation, sys, root_factory);
      if (!result)
        return caf::make_message(result.error());
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
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(node, caf::exit_reason::user_shutdown);
        else
          self->send(node, atom::signal_v, signal);
      })
    .until([&] {
      return stop;
    });
  return caf::make_message(std::move(err));
}

} // namespace vast::system
