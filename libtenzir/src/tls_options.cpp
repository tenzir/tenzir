//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tls_options.hpp"

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

auto parse_tls_version(std::string_view version) -> caf::expected<tls_version> {
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

template <typename T>
auto query_config(std::string_view name, operator_control_plane* ctrl)
  -> const T* {
  if (not ctrl) {
    return nullptr;
  }
  auto& config = ctrl->self().system().config();
  return caf::get_if<T>(&config.content, name);
}

template <typename T>
auto query_config_or_null(std::string_view name, operator_control_plane* ctrl)
  -> std::optional<located<T>> {
  if (auto* x = query_config<T>(name, ctrl)) {
    return located{*x, location::unknown};
  }
  return std::nullopt;
}

} // namespace

auto parse_curl_tls_version(std::string_view version) -> caf::expected<long> {
  TRY(auto parsed, parse_tls_version(version));
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
  TENZIR_UNREACHABLE();
}

auto parse_openssl_tls_version(std::string_view version) -> caf::expected<int> {
  TRY(auto parsed, parse_tls_version(version));
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
  TENZIR_UNREACHABLE();
}

auto parse_caf_tls_version(std::string_view version)
  -> caf::expected<caf::net::ssl::tls> {
  TRY(auto parsed, parse_tls_version(version));
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
  TENZIR_UNREACHABLE();
}

auto tls_options::add_tls_options(argument_parser2& parser) -> void {
  parser.named("tls", tls_)
    .named("skip_peer_verification", skip_peer_verification_)
    .named("cacert", cacert_)
    .named("certfile", certfile_)
    .named("keyfile", keyfile_)
    .named("tls_minium_version", tls_min_version_)
    .named("tls_ciphers", tls_ciphers_);
  if (is_server_) {
    parser.named("tls_client_ca", tls_client_ca_)
      .named("tls_require_client_cert", tls_require_client_cert_);
  }
}

auto tls_options::validate(diagnostic_handler& dh) const -> failure_or<void> {
  const auto check_option
    = [&](auto& thing, std::string_view name) -> failure_or<void> {
    if (tls_ and not tls_->inner and thing) {
      diagnostic::error("`{}` requires TLS", name)
        .primary(tls_->source, "TLS is disabled")
        .primary(*thing)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(check_option(skip_peer_verification_, "skip_peer_verification"));
  TRY(check_option(cacert_, "cacert"));
  TRY(check_option(certfile_, "certfile"));
  TRY(check_option(keyfile_, "keyfile"));
  TRY(check_option(tls_client_ca_, "tls_client_ca"));
  TRY(check_option(tls_require_client_cert_, "tls_require_client_cert"));
  if (get_tls(nullptr).inner
      and not get_skip_peer_verification(nullptr).inner) {
    if (cacert_ and not std::filesystem::exists(cacert_->inner)) {
      diagnostic::error("the configured CA certificate bundle does not exist")
        .note("configured location: `{}`", cacert_->inner)
        .primary(*cacert_)
        .emit(dh);
      return failure::promise();
    }
  }
  // Validate mTLS options
  if (tls_require_client_cert_ and tls_require_client_cert_->inner
      and not tls_client_ca_) {
    diagnostic::error(
      "`tls_require_client_cert` requires `tls_client_ca` to be set")
      .primary(*tls_require_client_cert_)
      .emit(dh);
    return failure::promise();
  }
  if (tls_client_ca_ and not std::filesystem::exists(tls_client_ca_->inner)) {
    diagnostic::error("the configured client CA certificate does not exist")
      .note("configured location: `{}`", tls_client_ca_->inner)
      .primary(*tls_client_ca_)
      .emit(dh);
    return failure::promise();
  }
  if (skip_peer_verification_) {
    diagnostic::warning(
      "skipping peer verification allows man in the middle attacks")
      .hint("consider using a private CA")
      .emit(dh);
  }
  return {};
}

auto tls_options::validate(const located<std::string>& url,
                           diagnostic_handler& dh) const -> failure_or<void> {
  return validate(url.inner, url.source, dh);
}

auto tls_options::validate(std::string_view url, location url_loc,
                           diagnostic_handler& dh) const -> failure_or<void> {
  const auto url_says_safe = url.starts_with("https://")
                             or url.starts_with("ftps://")
                             or url.starts_with("smtps://");
  const auto url_says_unsafe = url.starts_with("http://")
                               or url.starts_with("ftp://")
                               or url.starts_with("smtp://");
  if ((url_says_safe and tls_ and not tls_->inner)
      or (url_says_unsafe and tls_ and tls_->inner)) {
    diagnostic::error("conflicting TLS settings")
      .primary(url_loc, "url {} TLS", url_says_safe ? "enables" : "disables")
      .primary(tls_->source, "option {} TLS",
               tls_->inner ? "enables" : "disables")
      .emit(dh);
    return failure::promise();
  }
  return validate(dh);
}

auto tls_options::update_from_config(operator_control_plane& ctrl) -> void {
  tls_ = get_tls(&ctrl);
  skip_peer_verification_ = get_skip_peer_verification(&ctrl);
  cacert_ = get_cacert(&ctrl);
  certfile_ = get_certfile(&ctrl);
  keyfile_ = get_keyfile(&ctrl);
  tls_min_version_ = get_tls_min_version(&ctrl);
  tls_ciphers_ = get_tls_ciphers(&ctrl);
  tls_client_ca_ = get_tls_client_ca(&ctrl);
  tls_require_client_cert_ = get_tls_require_client_cert(&ctrl);
}

auto tls_options::get_tls(operator_control_plane* ctrl) const -> located<bool> {
  if (tls_) {
    return *tls_;
  }
  if (auto* x = query_config<bool>("tenzir.operator-tls.enable", ctrl)) {
    return {*x, location::unknown};
  }
  return {true, location::unknown};
}

auto tls_options::get_skip_peer_verification(operator_control_plane* ctrl) const
  -> located<bool> {
  if (skip_peer_verification_) {
    return *skip_peer_verification_;
  }
  if (auto* x = query_config<bool>("tenzir.operator-tls.skip-peer-verification",
                                   ctrl)) {
    return {*x, location::unknown};
  }
  return {false, location::unknown};
}

auto tls_options::get_cacert(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (cacert_) {
    return cacert_;
  }
  if (auto x = query_config<std::string>("tenzir.operator-tls.cacert", ctrl);
      x and not x->empty()) {
    return located{*x, location::unknown};
  }
  return query_config_or_null<std::string>("tenzir.cacert", ctrl);
}

auto tls_options::get_certfile(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (certfile_) {
    return certfile_;
  }
  return query_config_or_null<std::string>("tenzir.operator-tls.certfile",
                                           ctrl);
}

auto tls_options::get_keyfile(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (keyfile_) {
    return *keyfile_;
  }
  return query_config_or_null<std::string>("tenzir.operator-tls.keyfile", ctrl);
}

auto tls_options::get_tls_min_version(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (tls_min_version_) {
    return *keyfile_;
  }
  return query_config_or_null<std::string>(
    "tenzir.operator-tls.tls-min-version", ctrl);
}

auto tls_options::get_tls_ciphers(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (tls_ciphers_) {
    return *keyfile_;
  }
  return query_config_or_null<std::string>("tenzir.operator-tls.tls-ciphers",
                                           ctrl);
}

auto tls_options::get_tls_client_ca(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  if (tls_client_ca_) {
    return *keyfile_;
  }
  return query_config_or_null<std::string>("tenzir.operator-tls.tls-client-ca",
                                           ctrl);
}

auto tls_options::get_tls_require_client_cert(operator_control_plane* ctrl) const
  -> located<bool> {
  if (tls_require_client_cert_) {
    return *tls_require_client_cert_;
  }
  if (auto* x
      = query_config<bool>("tenzir.operator-tls.require-client-ca", ctrl)) {
    return {*x, location::unknown};
  }
  return {false, location::unknown};
}

auto tls_options::update_url(std::string_view url,
                             operator_control_plane* ctrl) const
  -> std::string {
  auto url_copy = std::string{url};
  if (not uses_curl_http_) {
    return url_copy;
  }
  auto tls_opt = get_tls(ctrl);
  if (not tls_opt.inner) {
    return url_copy;
  }
  /// If the url says http, and the TLS option was not defaulted
  if (url.starts_with("http://") and tls_opt.source != location::unknown) {
    url_copy.insert(4, "s");
  }
  return url_copy;
}

auto tls_options::apply_to(curl::easy& easy, std::string_view url,
                           operator_control_plane* ctrl) const -> caf::error {
  auto used_url = update_url(url, ctrl);
  check(easy.set(CURLOPT_URL, used_url));
  const auto tls_opt = get_tls(ctrl);
  if (tls_opt.inner) {
    check(easy.set(CURLOPT_DEFAULT_PROTOCOL, "https"));
  }
  if (auto x = get_cacert(ctrl)) {
    if (auto ec = easy.set(CURLOPT_CAINFO, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `cacert`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  if (auto x = get_certfile(ctrl)) {
    if (auto ec = easy.set(CURLOPT_SSLCERT, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `certfile`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  if (auto x = get_keyfile(ctrl)) {
    if (auto ec = easy.set(CURLOPT_SSLKEY, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `keyfile`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  check(easy.set(CURLOPT_USE_SSL,
                 tls_opt.inner ? CURLUSESSL_ALL : CURLUSESSL_NONE));
  const auto skip_peer_verification = get_skip_peer_verification(ctrl).inner;
  check(easy.set(CURLOPT_SSL_VERIFYPEER, skip_peer_verification ? 0 : 1));
  check(easy.set(CURLOPT_SSL_VERIFYHOST, skip_peer_verification ? 0 : 1));

  if (auto x = get_tls_min_version(ctrl)) {
    auto curl_version = parse_curl_tls_version(x->inner);
    if (curl_version) {
      check(easy.set(CURLOPT_SSLVERSION, *curl_version));
    } else {
      return diagnostic::error(curl_version.error()).primary(*x).to_error();
    }
  }
  if (auto x = get_tls_ciphers(ctrl)) {
    check(easy.set(CURLOPT_SSL_CIPHER_LIST, x->inner));
  }
  return {};
}

} // namespace tenzir
