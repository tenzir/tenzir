//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/start_command.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/application.hpp"
#include "tenzir/command.hpp"
#include "tenzir/concept/parseable/numeric/bool.hpp"
#include "tenzir/concept/parseable/tenzir/endpoint.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/endpoint.hpp"
#include "tenzir/error.hpp"
#include "tenzir/io/write.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/spawn_node.hpp"
#include "tenzir/systemd.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/io/middleman.hpp>
#include <caf/openssl/all.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>

namespace tenzir {

namespace {

/// Actor to run one of the additional commands given as
/// parameters to the Tenzir node.
using command_runner_actor = typed_actor_fwd<
  // Handle a request.
  auto(atom::run, invocation)->caf::result<void>>::unwrap;

auto command_runner(command_runner_actor::pointer self)
  -> command_runner_actor::behavior_type {
  return {
    [self](atom::run, tenzir::invocation& invocation) -> caf::result<void> {
      auto [root, root_factory] = make_application("tenzir-ctl");
      auto result = run(invocation, self->home_system(), root_factory);
      if (!result) {
        TENZIR_ERROR("failed to run start command {}: {}", invocation,
                     result.error());
      }
      return {};
    },
  };
}

} // namespace

using namespace std::chrono_literals;

auto start_command(const invocation& inv, caf::actor_system& sys)
  -> caf::message {
  TENZIR_TRACE_SCOPE("{} {}", TENZIR_ARG(inv.options),
                     TENZIR_ARG("args", inv.arguments.begin(),
                                inv.arguments.end()));
  auto node_endpoint = std::optional<endpoint>{};
  auto listen_endpoint = std::optional<std::string>{};
  const auto* endpoint_enabled = get_if<bool>(&inv.options, "tenzir.endpoint");
  if (endpoint_enabled) {
    if (*endpoint_enabled) {
      node_endpoint.emplace(defaults::endpoint_host.data());
    }
  } else {
    // Construct an endpoint.
    auto str
      = get_or(inv.options, "tenzir.endpoint", defaults::endpoint.data());
    // The endpoint option is defined in CAF's old option parser, and that
    // has no notion of options that can either be a boolean or a string, so we
    // need to correct this here.
    if (auto endpoint_enabled = false;
        parsers::boolean(str, endpoint_enabled)) {
      if (endpoint_enabled) {
        node_endpoint.emplace(defaults::endpoint_host.data());
      }
    } else {
      node_endpoint.emplace();
      if (not parsers::endpoint(str, *node_endpoint)) {
        return caf::make_message(
          diagnostic::error("invalid endpoint: {}", str).to_error());
      }
    }
  }
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Spawn our node.
  auto node_opt = spawn_node(self);
  if (!node_opt) {
    return caf::make_message(std::move(node_opt.error()));
  }
  auto const& node = node_opt->get();
  if (node_endpoint) {
    // Default host and port if they're not yet set.
    if (node_endpoint->host.empty()) {
      node_endpoint->host = std::string{defaults::endpoint_host};
    }
    if (not node_endpoint->port) {
      node_endpoint->port = port{defaults::endpoint_port, port_type::tcp};
    }
    auto publish = [&]() -> caf::expected<uint16_t> {
      const auto reuse_address = true;
      if (sys.has_openssl_manager()) {
        return caf::openssl::publish(node, node_endpoint->port->number(),
                                     node_endpoint->host.c_str(),
                                     reuse_address);
      }
      auto& mm = sys.middleman();
      return mm.publish(node, node_endpoint->port->number(),
                        node_endpoint->host.c_str(), reuse_address);
    };
    auto bound_port = publish();
    if (!bound_port) {
      auto err
        = diagnostic::error("failed to bind to port {}",
                            node_endpoint->port->number())
            .note("{}", bound_port.error())
            .hint("check for other running tenzir-node processes at port {}",
                  node_endpoint->port->number())
            .to_error();
      return caf::make_message(std::move(err));
    }
    listen_endpoint = fmt::format("{}:{}", node_endpoint->host, *bound_port);
    TENZIR_INFO("node listens for node-to-node connections on tcp://{}",
                *listen_endpoint);
    // A single line of output to publish out address for scripts.
    if (const auto* path = caf::get_if<std::string>(
          &inv.options, "tenzir.start.write-endpoint")) {
      if (auto err = io::write(*path, as_bytes(*listen_endpoint))) {
        TENZIR_WARN("failed to write listen_endpoint to {}: {}", *path, err);
      }
    }
    if (caf::get_or(inv.options, "tenzir.start.print-endpoint", false)) {
      // We're not using fmt::print here because it doesn't flush the stream.
      std::cout << *listen_endpoint << std::endl;
    }
  }
  // Notify the service manager if it expects an update.
  if (auto error = systemd::notify_ready()) {
    auto err = diagnostic::error("failed to signal readiness to systemd")
                 .note("{}", error)
                 .to_error();
    return caf::make_message(std::move(err));
  }
  // Run main loop.
  caf::error err;
  auto stop = false;
  self->monitor(node);
  auto commands = caf::get_or(inv.options, "tenzir.start.commands",
                              std::vector<std::string>{});
  if (commands.empty()) {
    if (auto const* command
        = caf::get_if<std::string>(&inv.options, "tenzir.start.commands")) {
      commands.push_back(*command);
    }
  }
  std::vector<command_runner_actor> command_runners;
  if (!commands.empty()) {
    auto [root, root_factory] = make_application("tenzir-ctl");
    // We're already in the start command, so we can safely assert that
    // make_application works as expected.
    TENZIR_ASSERT(root);
    for (auto const& command : commands) {
      // We use std::quoted for correct tokenization of quoted strings. The
      // invocation parser expects a vector of strings that are correctly
      // tokenized already.
      auto tokenizer = std::stringstream{command};
      auto cli = std::vector<std::string>{};
      auto current = std::string{};
      while (tokenizer >> std::quoted(current)) {
        cli.push_back(std::move(current));
      }
      TENZIR_INFO("running post-start command {}", command);
      auto hook_invocation = parse(*root, cli.begin(), cli.end());
      if (!hook_invocation) {
        return caf::make_message(hook_invocation.error());
      }
      detail::merge_settings(inv.options, hook_invocation->options,
                             policy::merge_lists::yes);
      // In case the listen port option was set to 0 we need to set the
      // port that was allocated by the operating system here.
      if (listen_endpoint) {
        caf::put(hook_invocation->options, "tenzir.endpoint", *listen_endpoint);
      }
      auto runner = self->spawn<caf::detached>(command_runner);
      command_runners.push_back(runner);
      self->send(runner, atom::run_v, *hook_invocation);
    }
  }
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        TENZIR_ASSERT(msg.source == node);
        TENZIR_DEBUG("{} received DOWN from node", *self);
        stop = true;
        if (msg.reason != caf::exit_reason::user_shutdown) {
          err = std::move(msg.reason);
        }
      },
      [&](atom::signal, int signal) {
        TENZIR_DEBUG("{} got {}", *self, ::strsignal(signal));
        TENZIR_ASSERT(signal == SIGINT || signal == SIGTERM);
        self->send_exit(node, caf::exit_reason::user_shutdown);
      })
    .until([&] {
      return stop;
    });
  return caf::make_message(std::move(err));
}

} // namespace tenzir
