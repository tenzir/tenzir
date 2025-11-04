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

/// Parses a TLS version string (e.g., "1.2") and returns the corresponding
/// CURL SSL version constant.
auto parse_curl_tls_version(std::string_view version) -> caf::expected<long>;

/// Parses a TLS version string (e.g., "1.2") and returns the corresponding
/// OpenSSL version constant.
auto parse_openssl_tls_version(std::string_view version) -> caf::expected<int>;

/// Parses a TLS version string (e.g., "1.2") and returns the corresponding
/// CAF SSL/TLS enum value.
auto parse_caf_tls_version(std::string_view version)
  -> caf::expected<caf::net::ssl::tls>;

struct ssl_options {
  ssl_options();

  auto add_tls_options(argument_parser2&) -> void;

  auto validate(diagnostic_handler&) const -> failure_or<void>;

  /// Ensures the internal consistency of the options, additionally considering
  /// the scheme in the URL.
  auto validate(const located<std::string>& url,
                diagnostic_handler&) const -> failure_or<void>;
  /// Ensures the internal consistency of the options, additionally considering
  /// the scheme in the URL.
  auto validate(std::string_view url, location url_loc,
                diagnostic_handler&) const -> failure_or<void>;

  /// Applies the options to a `curl::easy` object, potentially using
  /// `cacert_fallback`, if none is set explicitly.
  auto apply_to(curl::easy& easy, std::string_view url,
                std::string_view cacert_fallback = {}) const -> caf::error;
  /// Applies the options to a `curl::easy` object, potentially getting
  /// `tenzir.cacert` as a `cacert_fallbacl` if none is set explicitly.
  auto apply_to(curl::easy& easy, std::string_view url,
                operator_control_plane&) const -> caf::error;
  /// Updates a URL using the `tls` option
  auto update_url(std::string_view url) const -> std::string;

  /// Queries `tenzir.cacert` from the config.
  static auto
  query_cacert_fallback(operator_control_plane& ctrl) -> std::string;

  /// Queries `tenzir.tls.min-version` from the config.
  static auto
  query_tls_min_version(operator_control_plane& ctrl) -> std::string;

  /// Queries `tenzir.tls.ciphers` from the config.
  static auto query_tls_ciphers(operator_control_plane& ctrl) -> std::string;

  /// updates this->cacert to `tenzir.cacert` from the config, if it is not
  /// already set.
  auto update_cacert(operator_control_plane& ctrl) -> void;

  /// Get the value of the TLS option, or the config setting
  auto get_tls() const -> located<bool> {
    if (tls) {
      return *tls;
    }
    return {true, location::unknown};
  }

  auto get_skip_peer_verification() -> located<bool> {
    if (skip_peer_verification) {
      return {true, *skip_peer_verification};
    }
    return {false, location::unknown};
  }

  auto get_cacert() const -> located<std::string> {
    if (cacert) {
      return *cacert;
    }
    return {"", location::unknown};
  }

  auto get_certfile() const -> located<std::string> {
    if (certfile) {
      return *certfile;
    }
    return {"", location::unknown};
  }

  auto get_keyfile() const -> located<std::string> {
    if (keyfile) {
      return *keyfile;
    }
    return {"", location::unknown};
  }

  /// TODO: @iyeonline These should be private by design?
  bool uses_curl_http = false;
  std::optional<located<bool>> tls;
  std::optional<location> skip_peer_verification;
  std::optional<located<std::string>> cacert;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> tls_client_ca;
  std::optional<located<bool>> tls_require_client_cert;

  friend auto inspect(auto& f, ssl_options& x) -> bool {
    return f.object(x).fields(
      f.field("uses_curl_http", x.uses_curl_http), f.field("tls", x.tls),
      f.field("skip_peer_verification", x.skip_peer_verification),
      f.field("cacert", x.cacert), f.field("certfile", x.certfile),
      f.field("keyfile", x.keyfile), f.field("tls_client_ca", x.tls_client_ca),
      f.field("tls_require_client_cert", x.tls_require_client_cert));
  }
};

} // namespace tenzir
