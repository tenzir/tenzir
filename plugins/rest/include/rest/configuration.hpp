//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/type.hpp>

#include <string>

namespace vast::plugins::rest {

// The configuration that can be set by the user via
// YAML or command-line options.
struct configuration {
  static const record_type& layout() noexcept {
    static auto result = vast::record_type{
      {"bind", vast::string_type{}},    {"port", vast::count_type{}},
      {"mode", vast::string_type{}},    {"certfile", vast::string_type{}},
      {"keyfile", vast::string_type{}},
    };
    return result;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(caf::meta::type_name("vast.plugins.rest.configuration"),
             x.bind_address, x.port, x.mode, x.certfile, x.keyfile);
  }

  enum class server_mode {
    debug,
    server,
    upstream,
    mtls,
  };

  std::string mode = "debug"; // FIXME: switch default to 'server'
  std::string certfile = {};
  std::string keyfile = {};
  std::string bind_address = "127.0.0.1";
  uint16_t port = 42001;
};

// The resolved and validated configuration that gets used at runtime.
class server_config {
public:
  bool require_tls = true;
  bool require_clientcerts = false;
  bool require_authentication = true;
  bool require_localhost = true;
  // TODO: Actually load the file contents during config validation
  // and store them here. (and look up if the key needs to be kept
  // in memory after the initial server setup)
  std::filesystem::path certfile = {};
  std::filesystem::path keyfile = {};
  std::string bind_address = {};
  uint16_t port = {};
};

/// Validate that the user-provided configuration makes sense.
caf::expected<server_config> convert_and_validate(configuration);

} // namespace vast::plugins::rest
