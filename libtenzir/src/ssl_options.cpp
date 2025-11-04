//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ssl_options.hpp"

#include "tenzir/diagnostics.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/net/ssl/context.hpp>
#include <curl/curl.h>
#include <openssl/ssl.h>

#include <filesystem>

namespace tenzir {

namespace {

enum class tls_version {
  v1_0,
  v1_1,
  v1_2,
  v1_3,
};

auto parse_tls_version_string(std::string_view version)
  -> caf::expected<tls_version> {
  if (version == "1.0") {
    return tls_version::v1_0;
  }
  if (version == "1.1") {
    return tls_version::v1_1;
  }
  if (version == "1.2") {
    return tls_version::v1_2;
  }
  if (version == "1.3") {
    return tls_version::v1_3;
  }
  return caf::make_error(ec::invalid_argument,
                         fmt::format("invalid TLS version '{}', expected one "
                                     "of: 1.0, 1.1, 1.2, 1.3",
                                     version));
}

} // namespace

auto parse_curl_tls_version(std::string_view version) -> caf::expected<long> {
  TRY(auto parsed, parse_tls_version_string(version));
  switch (parsed) {
    case tls_version::v1_0:
      return CURL_SSLVERSION_TLSv1_0;
    case tls_version::v1_1:
      return CURL_SSLVERSION_TLSv1_1;
    case tls_version::v1_2:
      return CURL_SSLVERSION_TLSv1_2;
    case tls_version::v1_3:
      return CURL_SSLVERSION_TLSv1_3;
  }
  __builtin_unreachable();
}

auto parse_openssl_tls_version(std::string_view version) -> caf::expected<int> {
  TRY(auto parsed, parse_tls_version_string(version));
  switch (parsed) {
    case tls_version::v1_0:
      return TLS1_VERSION;
    case tls_version::v1_1:
      return TLS1_1_VERSION;
    case tls_version::v1_2:
      return TLS1_2_VERSION;
    case tls_version::v1_3:
      return TLS1_3_VERSION;
  }
  __builtin_unreachable();
}

auto parse_caf_tls_version(std::string_view version)
  -> caf::expected<caf::net::ssl::tls> {
  TRY(auto parsed, parse_tls_version_string(version));
  switch (parsed) {
    case tls_version::v1_0:
      return caf::net::ssl::tls::v1_0;
    case tls_version::v1_1:
      return caf::net::ssl::tls::v1_1;
    case tls_version::v1_2:
      return caf::net::ssl::tls::v1_2;
    case tls_version::v1_3:
      return caf::net::ssl::tls::v1_3;
  }
  __builtin_unreachable();
}

ssl_options::ssl_options() = default;

auto ssl_options::add_tls_options(argument_parser2& parser) -> void {
  parser.named("tls", tls)
    .named("skip_peer_verification", skip_peer_verification)
    .named("cacert", cacert)
    .named("certfile", certfile)
    .named("keyfile", keyfile)
    .named("tls_client_ca", tls_client_ca)
    .named("tls_require_client_cert", tls_require_client_cert);
}

auto ssl_options::validate(diagnostic_handler& dh) const -> failure_or<void> {
  const auto check_option
    = [&](auto& thing, std::string_view name) -> failure_or<void> {
    if (tls and not tls->inner and thing) {
      diagnostic::error("`{}` requires TLS", name)
        .primary(tls->source, "TLS is disabled")
        .primary(*thing)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(check_option(skip_peer_verification, "skip_peer_verification"));
  TRY(check_option(cacert, "cacert"));
  TRY(check_option(certfile, "certfile"));
  TRY(check_option(keyfile, "keyfile"));
  TRY(check_option(tls_client_ca, "tls_client_ca"));
  TRY(check_option(tls_require_client_cert, "tls_require_client_cert"));
  if (tls and tls->inner and not skip_peer_verification) {
    if (cacert and not std::filesystem::exists(cacert->inner)) {
      diagnostic::error("the configured CA certificate bundle does not exist")
        .note("configured location: `{}`", cacert->inner)
        .primary(*cacert)
        .emit(dh);
      return failure::promise();
    }
  }
  // Validate mTLS options
  if (tls_require_client_cert and tls_require_client_cert->inner
      and not tls_client_ca) {
    diagnostic::error(
      "`tls_require_client_cert` requires `tls_client_ca` to be set")
      .primary(*tls_require_client_cert)
      .emit(dh);
    return failure::promise();
  }
  if (tls_client_ca and not std::filesystem::exists(tls_client_ca->inner)) {
    diagnostic::error("the configured client CA certificate does not exist")
      .note("configured location: `{}`", tls_client_ca->inner)
      .primary(*tls_client_ca)
      .emit(dh);
    return failure::promise();
  }
  if (skip_peer_verification) {
    diagnostic::warning(
      "skipping peer verification allows man in the middle attacks")
      .hint("consider using a private CA")
      .emit(dh);
  }
  return {};
}

auto ssl_options::validate(const located<std::string>& url,
                           diagnostic_handler& dh) const -> failure_or<void> {
  return validate(url.inner, url.source, dh);
}

auto ssl_options::validate(std::string_view url, location url_loc,
                           diagnostic_handler& dh) const -> failure_or<void> {
  const auto url_says_safe = url.starts_with("https://")
                             or url.starts_with("ftps://")
                             or url.starts_with("smtps://");
  const auto url_says_unsafe = url.starts_with("http://")
                               or url.starts_with("ftp://")
                               or url.starts_with("smtp://");
  if ((url_says_safe and tls and not tls->inner)
      or (url_says_unsafe and tls and tls->inner)) {
    diagnostic::error("conflicting TLS settings")
      .primary(url_loc, "url {} TLS", url_says_safe ? "enables" : "disables")
      .primary(tls->source, "option {} TLS",
               tls->inner ? "enables" : "disables")
      .emit(dh);
    return failure::promise();
  }
  return validate(dh);
}

auto ssl_options::update_url(std::string_view url) const -> std::string {
  auto url_copy = std::string{url};
  if (not uses_curl_http) {
    return url_copy;
  }
  auto tls_opt = get_tls();
  if (not tls_opt.inner) {
    return url_copy;
  }
  /// If the url says http, and the TLS option was not defaulted
  if (url.starts_with("http://") and tls_opt.source != location::unknown) {
    url_copy.insert(4, "s");
  }
  return url_copy;
}

auto ssl_options::apply_to(curl::easy& easy, std::string_view url,
                           std::string_view cacert_fallback) const
  -> caf::error {
  /// Update URL. This is crucial for CURL based connectors, as curl does not
  /// respect `CURLOPT_USE_SSL` for HTTP.
  auto used_url = update_url(url);
  check(easy.set(CURLOPT_URL, used_url));
  const auto tls_opt = get_tls();
  if (tls_opt.inner) {
    check(easy.set(CURLOPT_DEFAULT_PROTOCOL, "https"));
  }
  if (cacert) {
    if (auto ec = easy.set(CURLOPT_CAINFO, cacert->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `cacert`: {}", to_string(ec))
        .to_error();
    }
  } else if (not cacert_fallback.empty()) {
    if (auto ec = easy.set(CURLOPT_CAINFO, cacert_fallback);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `cacert`: {}", to_string(ec))
        .to_error();
    }
  }
  if (certfile) {
    if (auto ec = easy.set(CURLOPT_SSLCERT, certfile->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `certfile`: {}", to_string(ec))
        .to_error();
    }
  }
  if (keyfile) {
    if (auto ec = easy.set(CURLOPT_SSLKEY, keyfile->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `keyfile`: {}", to_string(ec))
        .to_error();
    }
  }
  check(easy.set(CURLOPT_USE_SSL,
                 tls_opt.inner ? CURLUSESSL_ALL : CURLUSESSL_NONE));
  check(easy.set(CURLOPT_SSL_VERIFYPEER, skip_peer_verification ? 0 : 1));
  check(easy.set(CURLOPT_SSL_VERIFYHOST, skip_peer_verification ? 0 : 1));
  return {};
}

auto ssl_options::apply_to(curl::easy& easy, std::string_view url,
                           operator_control_plane& ctrl) const -> caf::error {
  // Apply base options (including cacert fallback)
  if (not cacert.has_value()) {
    TRY(apply_to(easy, url, query_cacert_fallback(ctrl)));
  } else {
    TRY(apply_to(easy, url));
  }

  // Apply TLS min version from config
  auto min_version_str = query_tls_min_version(ctrl);
  if (not min_version_str.empty()) {
    auto curl_version = parse_curl_tls_version(min_version_str);
    if (curl_version) {
      check(easy.set(CURLOPT_SSLVERSION, *curl_version));
    } else {
      diagnostic::warning(curl_version.error())
        .note("while applying TLS configuration")
        .emit(ctrl.diagnostics());
    }
  }

  // Apply cipher list from config
  auto ciphers = query_tls_ciphers(ctrl);
  if (not ciphers.empty()) {
    check(easy.set(CURLOPT_SSL_CIPHER_LIST, ciphers));
  }

  return {};
}

auto ssl_options::query_cacert_fallback(operator_control_plane& ctrl)
  -> std::string {
  auto& config = ctrl.self().system().config();
  if (auto* v = caf::get_if<std::string>(&config.content, "tenzir.cacert")) {
    return *v;
  }
  return {};
}

auto ssl_options::query_tls_min_version(operator_control_plane& ctrl)
  -> std::string {
  auto& config = ctrl.self().system().config();
  if (auto* v
      = caf::get_if<std::string>(&config.content, "tenzir.tls.min-version")) {
    return *v;
  }
  return {};
}

auto ssl_options::query_tls_ciphers(operator_control_plane& ctrl)
  -> std::string {
  auto& config = ctrl.self().system().config();
  if (auto* v
      = caf::get_if<std::string>(&config.content, "tenzir.tls.ciphers")) {
    return *v;
  }
  return {};
}

auto ssl_options::update_cacert(operator_control_plane& ctrl) -> void {
  if (not cacert.has_value()) {
    cacert = located{query_cacert_fallback(ctrl), location::unknown};
  }
}

} // namespace tenzir
