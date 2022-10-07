//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <web/configuration.hpp>

namespace vast::plugins::web {

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
  // This doesn't help against TOCTOU errors, but at least it
  // catches obvious ones.
  if (!result.certfile.empty() && !std::filesystem::exists(result.certfile))
    return caf::make_error(ec::invalid_argument,
                           fmt::format("file not found: {}", config.certfile));
  if (!result.keyfile.empty() && !std::filesystem::exists(result.keyfile))
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

} // namespace vast::plugins::web
