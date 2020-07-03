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

#pragma once

#include "vast/command.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/detail/string.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/format/reader.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/datagram_source.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/io/middleman.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <csignal>
#include <string>
#include <type_traits>
#include <utility>

namespace vast::system {

namespace {

caf::expected<expression> parse_expression(command::argument_iterator begin,
                                           command::argument_iterator end) {
  auto str = detail::join(begin, end, " ");
  auto expr = to<expression>(str);
  if (expr)
    expr = normalize_and_validate(*expr);
  return expr;
}

} // namespace

template <class Reader, class Defaults>
caf::message import_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv.full_name, VAST_ARG("options", inv.options), VAST_ARG(sys));
  // Placeholder thingies.
  auto self = caf::scoped_actor{sys};
  auto err = caf::error{};
  auto src = caf::actor{};
  auto udp_port = std::optional<uint16_t>{};
  auto reader = std::unique_ptr<Reader>{nullptr};
  // Parse options.
  auto& options = inv.options;
  std::string category = Defaults::category;
  auto max_events = caf::get_if<size_t>(&options, "import.max-events");
  auto uri = caf::get_if<std::string>(&options, category + ".listen");
  auto file = caf::get_if<std::string>(&options, category + ".read");
  [[maybe_unused]] auto uds = get_or(options, category + ".uds", false);
  auto type = caf::get_if<std::string>(&options, category + ".type");
  auto slice_type = get_or(options, "import.table-slice-type",
                           defaults::import::table_slice_type);
  auto slice_size = get_or(options, "import.table-slice-size",
                           defaults::import::table_slice_size);
  if (slice_size == 0)
    return caf::make_message(
      make_error(ec::invalid_configuration, "table-slice-size can't be 0"));
  // Parse schema local to the import command.
  auto schema = get_schema(options, category);
  if (!schema)
    return caf::make_message(schema.error());
  // Discern the input source (file, stream, or socket).
  if (uri && file)
    return caf::make_message(make_error(ec::invalid_configuration,
                                        "only one source possible (-r or -l)"));
  if (!uri && !file) {
    using inputs = vast::format::reader::inputs;
    if constexpr (Reader::defaults::input == inputs::inet)
      uri = std::string{Reader::defaults::uri};
    else
      file = std::string{Reader::defaults::path};
  }
  if (uri) {
    endpoint ep;
    if (!vast::parsers::endpoint(*uri, ep))
      return caf::make_message(
        make_error(vast::ec::parse_error, "unable to parse endpoint", *uri));
    if (ep.port.type() == port::unknown) {
      using inputs = vast::format::reader::inputs;
      if constexpr (Reader::defaults::input == inputs::inet) {
        endpoint default_ep;
        vast::parsers::endpoint(Reader::defaults::uri, default_ep);
        ep.port = port{ep.port.number(), default_ep.port.type()};
      } else {
        // Fall back to tcp if we don't know anything else.
        ep.port = port{ep.port.number(), port::tcp};
      }
    }
    reader = std::make_unique<Reader>(slice_type, options);
    VAST_INFO(*reader, "listens for data on", ep.host, ", port", ep.port);
    switch (ep.port.type()) {
      default:
        return caf::make_message(make_error(
          vast::ec::unimplemented, "port type not supported:", ep.port.type()));
      case port::udp:
        udp_port = ep.port.number();
        break;
    }
  } else {
    auto in = detail::make_input_stream(*file, uds);
    if (!in)
      return caf::make_message(std::move(in.error()));
    reader = std::make_unique<Reader>(slice_type, options, std::move(*in));
    if (*file == "-")
      VAST_INFO(*reader, "reads data from stdin");
    else
      VAST_INFO(*reader, "reads data from", *file);
  }
  if (!reader)
    return caf::make_message(make_error(ec::invalid_result, "failed to spawn "
                                                            "reader"));
  VAST_VERBOSE(*reader, "produces", slice_type, "table slices of", slice_size,
               "events");
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(inv.full_name, "got node");
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Get node components.
  auto components = get_node_components(
    self, node, {"accountant", "type-registry", "importer"});
  if (!components)
    return make_message(components.error());
  VAST_ASSERT(components->size() == 3);
  auto accountant = caf::actor_cast<accountant_type>((*components)[0]);
  auto type_registry = caf::actor_cast<type_registry_type>((*components)[1]);
  auto& importer = (*components)[2];
  if (!type_registry)
    return make_message(make_error(ec::missing_component, "type-registry"));
  // Spawn the source, falling back to the default spawn function.
  auto local_schema = schema ? std::move(*schema) : vast::schema{};
  auto type_filter = type ? std::move(*type) : std::string{};
  if (udp_port) {
    auto& mm = sys.middleman();
    src = mm.spawn_broker(datagram_source<Reader>, *udp_port,
                          std::move(*reader), slice_size, max_events,
                          std::move(type_registry), std::move(local_schema),
                          std::move(type_filter), std::move(accountant));
  } else {
    src = sys.spawn(source<Reader>, std::move(*reader), slice_size, max_events,
                    std::move(type_registry), std::move(local_schema),
                    std::move(type_filter), std::move(accountant));
  }
  VAST_ASSERT(src);
  // Attempt to parse the remainder as an expression.
  if (!inv.arguments.empty()) {
    auto expr = parse_expression(inv.arguments.begin(), inv.arguments.end());
    if (!expr)
      return make_message(std::move(expr.error()));
    self->send(src, std::move(*expr));
  }
  // Connect source to importer.
  if (!importer)
    return make_message(make_error(ec::missing_component, "importer"));
  VAST_DEBUG(inv.full_name, "connects to", VAST_ARG("importer", importer));
  self->send(src, atom::sink_v, importer);
  // Start the source.
  bool stop = false;
  self->monitor(src);
  self->monitor(importer);
  self
    ->do_receive(
      [&](const caf::down_msg& msg) {
        if (msg.source == importer) {
          VAST_DEBUG(inv.full_name, "received DOWN from node importer");
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG(inv.full_name, "received DOWN from source");
          if (caf::get_or(inv.options, "import.blocking", false))
            self->send(importer, atom::subscribe_v, atom::flush::value, self);
          else
            stop = true;
        } else {
          VAST_DEBUG(inv.full_name, "received unexpected DOWN from",
                     msg.source);
          VAST_ASSERT(!"unexpected DOWN message");
        }
      },
      [&](atom::flush) {
        VAST_DEBUG(inv.full_name, "received flush from IMPORTER");
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG(inv.full_name, "received signal", ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(src, caf::exit_reason::user_shutdown);
      })
    .until(stop);
  if (err)
    return make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
