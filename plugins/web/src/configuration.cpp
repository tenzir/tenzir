//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/data.hpp>
#include <tenzir/detail/installdirs.hpp>
#include <tenzir/error.hpp>
#include <tenzir/optional.hpp>

#include <fmt/std.h>
#include <web/configuration.hpp>

namespace tenzir::plugins::web {

static caf::expected<enum configuration::server_mode>
to_server_mode(const std::string& str) {
  if (str == "dev")
    return configuration::server_mode::dev;
  else if (str == "upstream")
    return configuration::server_mode::upstream;
  else if (str == "server")
    return configuration::server_mode::server;
  else if (str == "mtls")
    return configuration::server_mode::mtls;
  return caf::make_error(ec::invalid_argument, "unknown mode");
}

caf::expected<server_config> convert_and_validate(configuration config) {
  auto result = server_config{};
  auto mode = to_server_mode(config.mode);
  if (!mode)
    return mode.error();
  switch (*mode) {
    case configuration::server_mode::dev:
      result.require_tls = false;
      result.require_clientcerts = false;
      result.require_authentication = false;
      result.require_localhost = false;
      result.enable_detailed_errors = true;
      result.cors_allowed_origin.emplace("*");
      break;
    case configuration::server_mode::upstream:
      result.require_tls = false;
      result.require_clientcerts = false;
      result.require_authentication = true;
      result.require_localhost = true;
      break;
    case configuration::server_mode::mtls:
      result.require_tls = true;
      result.require_clientcerts = true;
      result.require_authentication = true;
      result.require_localhost = false;
      break;
    case configuration::server_mode::server:
      result.require_tls = true;
      result.require_clientcerts = false;
      result.require_authentication = true;
      result.require_localhost = false;
  }
  result.certfile = config.certfile;
  result.keyfile = config.keyfile;
  auto ec = std::error_code{};
  if (!config.web_root.empty()) {
    result.webroot = config.web_root;
    // This doesn't help against TOCTOU errors, but at least it
    // catches obvious ones.
    if (!is_directory(*result.webroot, ec))
      return caf::make_error(ec::invalid_argument,
                             fmt::format("directory not found: {}",
                                         result.webroot));
  } else {
    result.webroot = std::nullopt;
  }
  if (!result.certfile.empty() && !exists(result.certfile))
    return caf::make_error(ec::invalid_argument,
                           fmt::format("file not found: {}", config.certfile));
  if (!result.keyfile.empty() && !exists(result.keyfile))
    return caf::make_error(ec::invalid_argument,
                           fmt::format("file not found: {}", config.keyfile));
  if (result.require_tls)
    if (result.keyfile.empty() || result.certfile.empty())
      return caf::make_error(ec::invalid_argument, "either keyfile or certfile "
                                                   "argument is missing");
  result.bind_address = config.bind_address;
  if (result.require_localhost)
    if (result.bind_address != "localhost" && result.bind_address != "127.0.0.1"
        && result.bind_address != "::1")
      return caf::make_error(
        ec::invalid_argument,
        fmt::format("can only bind to localhost in {} mode", config.mode));
  result.port = config.port;
  return result;
}

caf::error convert(const tenzir::data& src, configuration& dst) {
  const auto* rec = try_as<tenzir::record>(&src);
  if (! rec) {
    return caf::make_error(ec::convert_error,
                           "expected record for web::configuration conversion");
  }
  dst.bind_address = get_or(*rec, "bind", dst.bind_address);
  if (const auto* port = get_if<int64_t>(rec, "port")) {
    dst.port = static_cast<int>(*port);
  }
  dst.mode = get_or(*rec, "mode", dst.mode);
  dst.certfile = get_or(*rec, "certfile", dst.certfile);
  dst.keyfile = get_or(*rec, "keyfile", dst.keyfile);
  dst.web_root = get_or(*rec, "web-root", dst.web_root);
  dst.cors_allowed_origin
    = get_or(*rec, "cors-allowed-origin", dst.cors_allowed_origin);
  return caf::none;
}

} // namespace tenzir::plugins::web
