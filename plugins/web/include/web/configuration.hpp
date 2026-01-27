//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/fwd.hpp>
#include <tenzir/type.hpp>

#include <caf/error.hpp>

#include <string>

namespace tenzir::plugins::web {

// The configuration that can be set by the user via
// YAML or command-line options.
struct configuration {
  static const record_type& schema() noexcept {
    static auto result = tenzir::record_type{
      {"bind", tenzir::string_type{}},
      {"port", tenzir::int64_type{}},
      {"mode", tenzir::string_type{}},
      {"certfile", tenzir::string_type{}},
      {"keyfile", tenzir::string_type{}},
      {"web-root", tenzir::string_type{}},
      {"cors-allowed-origin", tenzir::string_type{}},
    };
    return result;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f.object(x)
      .pretty_name("tenzir.plugins.rest.configuration")
      .fields(f.field("bind-address", x.bind_address), f.field("port", x.port),
              f.field("mode", x.mode), f.field("certfile", x.certfile),
              f.field("keyfile", x.keyfile), f.field("web-root", x.web_root));
  }

  enum class server_mode {
    dev,
    server,
    upstream,
    mtls,
  };

  std::string mode = "server";
  std::string certfile = {};
  std::string keyfile = {};
  std::string bind_address = "127.0.0.1";
  std::string web_root = {};
  std::string cors_allowed_origin = {};
  int port = 5160;
};

// The resolved and validated configuration that gets used at runtime.
class server_config {
public:
  /// The listen address of the server.
  std::string bind_address = {};

  /// The listen port of the server.
  uint16_t port = {};

  /// Whether the server should allow plain http requests.
  bool require_tls = true;

  /// Whether the server should require client certificates for
  /// incoming connections.
  bool require_clientcerts = false;

  /// Whether the server should require a valid authentication token
  /// for API requests.
  bool require_authentication = true;

  /// Whether to allow the server to bind to non-local addresses.
  bool require_localhost = true;

  /// Whether to include full error information in the response.
  bool enable_detailed_errors = false;

  /// The path to the TLS certificate.
  std::filesystem::path certfile = {};

  /// The path to the TLS private key.
  std::filesystem::path keyfile = {};

  /// Permit cross-site calls from this origin.
  //  If set, the server will insert a `Access-Control-Allow-Origin`
  //  header into every API response.
  std::optional<std::string> cors_allowed_origin = {};

  /// Additional headers to be inserted into every server response.
  //  (eg. 'Server: Tenzir 2.4', ...)
  std::unordered_map<std::string, std::string> response_headers;

  /// The path from which to serve static files.
  std::optional<std::filesystem::path> webroot = {};
};

/// Validate that the user-provided configuration makes sense.
caf::expected<server_config> convert_and_validate(configuration);

/// Converts data (record from YAML/config) to web::configuration.
/// This is a targeted conversion that avoids the expensive generic match()
/// in concept/convertible/data.hpp.
/// @param src The source data, expected to be a record.
/// @param dst The destination configuration to populate.
/// @returns An error if conversion fails, or caf::none on success.
caf::error convert(const tenzir::data& src, configuration& dst);

} // namespace tenzir::plugins::web
