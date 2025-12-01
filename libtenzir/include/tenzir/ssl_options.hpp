//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser2.hpp"
#include "tenzir/curl.hpp"

#include <caf/expected.hpp>
#include <caf/net/fwd.hpp>

#include <optional>
#include <string_view>

namespace tenzir {

auto parse_curl_tls_version(std::string_view version) -> caf::expected<long>;
auto parse_openssl_tls_version(std::string_view version) -> caf::expected<int>;

auto parse_caf_tls_version(std::string_view version)
  -> caf::expected<caf::net::ssl::tls>;

class ssl_options {
public:
  struct options {
    bool tls_default = true;
    bool uses_curl_http = false;
    bool is_server = false;
  };

  explicit ssl_options(options opts)
    : uses_curl_http_{opts.uses_curl_http},
      is_server_{opts.is_server},
      tls_{located{opts.tls_default, location::unknown}} {
  }
  ssl_options() = default;

  auto add_tls_options(argument_parser2&) -> void;

  auto validate(diagnostic_handler&) const -> failure_or<void>;

  /// Ensures the internal consistency of the options, additionally
  /// considering the scheme in the URL.
  auto validate(const located<std::string>& url, diagnostic_handler&) const
    -> failure_or<void>;
  /// Ensures the internal consistency of the options, additionally
  /// considering the scheme in the URL.
  auto validate(std::string_view url, location url_loc,
                diagnostic_handler&) const -> failure_or<void>;

  /// Applies the options to a `curl::easy` object, potentially getting
  /// `tenzir.cacert` as a `cacert_fallbacl` if none is set explicitly.
  auto apply_to(curl::easy& easy, std::string_view url,
                operator_control_plane* ctrl) const -> caf::error;

  /// Updates values in *this using the config.
  auto update_from_config(operator_control_plane& ctrl) -> void;

  /// Updates a URL using the `tls` option
  [[nodiscard]] auto
  update_url(std::string_view url, operator_control_plane* ctrl) const
    -> std::string;

  /// Get the value of the TLS option, or the config setting
  auto get_tls(operator_control_plane* ctrl) const -> located<bool>;

  auto get_skip_peer_verification(operator_control_plane* ctrl) const
    -> located<bool>;

  auto get_cacert(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_certfile(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_keyfile(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_tls_min_version(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_tls_ciphers(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_tls_client_ca(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;

  auto get_tls_require_client_cert(operator_control_plane* ctrl) const
    -> located<bool>;

private:
  bool uses_curl_http_ = false;
  bool is_server_ = false;
  std::optional<located<bool>> tls_;
  std::optional<located<bool>> skip_peer_verification_;
  std::optional<located<std::string>> cacert_;
  std::optional<located<std::string>> certfile_;
  std::optional<located<std::string>> keyfile_;
  std::optional<located<std::string>> tls_min_version_;
  std::optional<located<std::string>> tls_ciphers_;
  std::optional<located<std::string>> tls_client_ca_;
  std::optional<located<bool>> tls_require_client_cert_;

  friend auto inspect(auto& f, ssl_options& x) -> bool {
    return f.object(x).fields(
      f.field("uses_curl_http", x.uses_curl_http_),
      f.field("is_server", x.is_server_), f.field("tls", x.tls_),
      f.field("skip_peer_verification", x.skip_peer_verification_),
      f.field("cacert", x.cacert_), f.field("certfile", x.certfile_),
      f.field("keyfile", x.keyfile_), f.field("keyfile", x.keyfile_),
      f.field("tls_min_version", x.tls_min_version_),
      f.field("tls_ciphers", x.tls_ciphers_),
      f.field("tls_client_ca", x.tls_client_ca_),
      f.field("tls_require_client_cert", x.tls_require_client_cert_));
  }
};

} // namespace tenzir
