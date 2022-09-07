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
      {"keyfile", vast::string_type{}}, {"dhtmpfile", vast::string_type{}},
    };
    return result;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(caf::meta::type_name("vast.plugins.rest.configuration"),
             x.bind_address, x.port, x.mode, x.certfile, x.keyfile,
             x.dhtmpfile);
  }

  std::string mode;
  std::string certfile;
  std::string keyfile;
  std::string dhtmpfile;
  std::string bind_address = "127.0.0.1";
  uint16_t port = 42001;
};

// The resolved and validated configuration that gets used at runtime.
class server_config {
public:
  enum class mode {
    debug,
    server,
    upstream,
    mtls,
  };

  mode mode = {};
  std::filesystem::path certfile = {};
  std::filesystem::path keyfile = {};
  std::filesystem::path dhtmpfile = {};
  std::string bind_address = {};
  uint16_t port = {};
};

} // namespace vast::plugins::rest
