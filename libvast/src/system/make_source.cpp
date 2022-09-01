//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/make_source.hpp"

#include "vast/command.hpp"
#include "vast/component_config.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/parseable/vast/table_slice_encoding.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/port.hpp"
#include "vast/defaults.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/module.hpp"
#include "vast/optional.hpp"
#include "vast/system/datagram_source.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"

#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>
#include <caf/spawn_options.hpp>

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

template <class... Args>
void send_to_source(caf::actor& source, Args&&... args) {
  static_assert(
    caf::detail::tl_count_type<source_actor::signatures,
                               caf::reacts_to<std::decay_t<Args>...>>::value,
    "Args are incompatible with source actor's API");
  caf::anon_send(source, std::forward<Args>(args)...);
}

} // namespace

caf::expected<caf::actor>
make_source(caf::actor_system& sys, const std::string& format,
            const invocation& inv, accountant_actor accountant,
            type_registry_actor type_registry,
            stream_sink_actor<table_slice, std::string> importer,
            std::vector<pipeline>&& pipelines, bool detached) {
  if (!importer)
    return caf::make_error(ec::missing_component, "importer");
  // Placeholder thingies.
  auto udp_port = std::optional<uint16_t>{};
  // Parse options.
  const auto& options = inv.options;
  auto max_events
    = to_std(caf::get_if<size_t>(&options, "vast.import.max-events"));
  auto uri = caf::get_if<std::string>(&options, "vast.import.listen");
  auto file = caf::get_if<std::string>(&options, "vast.import.read");
  auto type = caf::get_if<std::string>(&options, "vast.import.type");
  auto encoding = defaults::import::table_slice_type;
  if (!extract_settings(encoding, options, "vast.import.batch-encoding"))
    return caf::make_error(ec::invalid_configuration, "failed to extract "
                                                      "batch-encoding option");
  VAST_ASSERT(encoding != table_slice_encoding::none);
  auto slice_size = caf::get_or(options, "vast.import.batch-size",
                                defaults::import::table_slice_size);
  if (slice_size == 0)
    slice_size = std::numeric_limits<decltype(slice_size)>::max();
  // Parse module local to the import command.
  auto module = get_module(options);
  if (!module)
    return module.error();
  // Discern the input source (file, stream, or socket).
  if (uri && file)
    return caf::make_error(ec::invalid_configuration, //
                           "only one source possible (-r or -l)");
  if (!uri && !file)
    file = std::string{defaults::import::read};
  if (uri) {
    endpoint ep;
    if (!vast::parsers::endpoint(*uri, ep))
      return caf::make_error(vast::ec::parse_error, "unable to parse endpoint",
                             *uri);
    if (!ep.port)
      return caf::make_error(vast::ec::invalid_configuration,
                             "endpoint does not "
                             "specify port");
    if (ep.port->type() == port_type::unknown)
      // Fall back to tcp if we don't know anything else.
      ep.port = port{ep.port->number(), port_type::tcp};
    VAST_INFO("{}-reader listens for data on {}", format,
              ep.host + ":" + to_string(*ep.port));
    switch (ep.port->type()) {
      default:
        return caf::make_error(vast::ec::unimplemented,
                               "port type not supported:", ep.port->type());
      case port_type::udp:
        udp_port = ep.port->number();
        break;
    }
  }
  auto reader = format::reader::make(format, inv.options);
  if (!reader)
    return reader.error();
  if (!*reader)
    return caf::make_error(ec::logic_error, fmt::format("the {} reader is not "
                                                        "registered with the "
                                                        "reader factory",
                                                        format));
  if (slice_size == std::numeric_limits<decltype(slice_size)>::max())
    VAST_VERBOSE("{} produces {} table slices", (*reader)->name(), encoding);
  else
    VAST_VERBOSE("{} produces {} table slices of at most {} events",
                 (*reader)->name(), encoding, slice_size);
  // Spawn the source, falling back to the default spawn function.
  auto local_module = module ? std::move(*module) : vast::module{};
  auto type_filter = type ? std::move(*type) : std::string{};
  auto src =
    [&](auto&&... args) {
      if (udp_port) {
        if (detached)
          return sys.middleman().spawn_broker<caf::spawn_options::detach_flag>(
            datagram_source, *udp_port, std::forward<decltype(args)>(args)...);
        return sys.middleman().spawn_broker(
          datagram_source, *udp_port, std::forward<decltype(args)>(args)...);
      }
      if (detached)
        return sys.spawn<caf::detached>(source,
                                        std::forward<decltype(args)>(args)...);
      return sys.spawn(source, std::forward<decltype(args)>(args)...);
    }(std::move(*reader), slice_size, max_events, std::move(type_registry),
      std::move(local_module), std::move(type_filter), std::move(accountant),
      std::move(pipelines));
  VAST_ASSERT(src);
  // Attempt to parse the remainder as an expression.
  if (!inv.arguments.empty()) {
    auto expr = parse_expression(inv.arguments.begin(), inv.arguments.end());
    if (!expr)
      return expr.error();
    send_to_source(src, atom::normalize_v, std::move(*expr));
  }
  // Connect source to importer.
  VAST_DEBUG("{} connects to {}", inv.full_name, VAST_ARG(importer));
  send_to_source(src, importer);
  return src;
}

} // namespace vast::system
