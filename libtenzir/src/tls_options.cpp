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

#include <algorithm>
#include <array>
#include <filesystem>

namespace tenzir {

namespace {

// Valid keys for the tls record (snake_case, no tls_ prefix)
constexpr std::array<std::string_view, 8> valid_tls_record_keys = {
  "skip_peer_verification",
  "cacert",
  "certfile",
  "keyfile",
  "min_version",
  "ciphers",
  "client_ca",
  "require_client_cert",
};

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

template <typename T>
constexpr auto inner(const std::optional<located<T>>& x) -> std::optional<T> {
  return x.transform([](auto&& x) {
    return x.inner;
  });
};

} // namespace

auto tls_options::get_record_bool(std::string_view key) const
  -> std::optional<located<bool>> {
  if (not tls_) {
    return std::nullopt;
  }
  const auto* rec = try_as<record>(&tls_->inner);
  if (not rec) {
    return std::nullopt;
  }
  auto it = rec->find(key);
  if (it == rec->end()) {
    return std::nullopt;
  }
  const auto* val = try_as<bool>(&it->second);
  if (not val) {
    return std::nullopt; // Type mismatch handled in validation
  }
  return located{*val, tls_->source};
}

auto tls_options::get_record_string(std::string_view key) const
  -> std::optional<located<std::string>> {
  if (not tls_) {
    return std::nullopt;
  }
  const auto* rec = try_as<record>(&tls_->inner);
  if (not rec) {
    return std::nullopt;
  }
  auto it = rec->find(key);
  if (it == rec->end()) {
    return std::nullopt;
  }
  const auto* val = try_as<std::string>(&it->second);
  if (not val) {
    return std::nullopt; // Type mismatch handled in validation
  }
  return located{*val, tls_->source};
}

auto tls_options::validate_tls_record(diagnostic_handler& dh) const
  -> failure_or<void> {
  if (not tls_) {
    return {};
  }
  // Handle bool case
  if (is<bool>(tls_->inner)) {
    return {};
  }
  // Handle record case
  const auto* rec = try_as<record>(&tls_->inner);
  if (not rec) {
    diagnostic::error("`tls` must be a bool or record")
      .primary(tls_->source)
      .emit(dh);
    return failure::promise();
  }
  // Check for unknown keys
  for (const auto& [key, value] : *rec) {
    bool found = std::find(valid_tls_record_keys.begin(),
                           valid_tls_record_keys.end(), key)
                 != valid_tls_record_keys.end();
    if (not found) {
      diagnostic::error("unknown key `{}` in `tls` record", key)
        .primary(tls_->source)
        .hint("valid keys are: {}", fmt::join(valid_tls_record_keys, ", "))
        .emit(dh);
      return failure::promise();
    }
  }
  // Type-check each key
  auto check_bool = [&](std::string_view key) -> failure_or<void> {
    auto it = rec->find(key);
    if (it != rec->end() and not is<bool>(it->second)) {
      diagnostic::error("`tls.{}` must be a bool", key)
        .primary(tls_->source)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  auto check_string = [&](std::string_view key) -> failure_or<void> {
    auto it = rec->find(key);
    if (it != rec->end() and not is<std::string>(it->second)) {
      diagnostic::error("`tls.{}` must be a string", key)
        .primary(tls_->source)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(check_bool("skip_peer_verification"));
  TRY(check_string("cacert"));
  TRY(check_string("certfile"));
  TRY(check_string("keyfile"));
  TRY(check_string("min_version"));
  TRY(check_string("ciphers"));
  TRY(check_string("client_ca"));
  TRY(check_bool("require_client_cert"));
  // Server-only keys validation
  if (not is_server_) {
    if (rec->find("client_ca") != rec->end()) {
      diagnostic::error("`tls.client_ca` is only valid for server mode")
        .primary(tls_->source)
        .emit(dh);
      return failure::promise();
    }
    if (rec->find("require_client_cert") != rec->end()) {
      diagnostic::error("`tls.require_client_cert` is only valid for server "
                        "mode")
        .primary(tls_->source)
        .emit(dh);
      return failure::promise();
    }
  }
  return {};
}

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
  parser.named("tls", tls_, "record")
    .named("skip_peer_verification", skip_peer_verification_)
    .named("cacert", cacert_)
    .named("certfile", certfile_)
    .named("keyfile", keyfile_)
    .named("password", password_);
}

auto tls_options::validate(diagnostic_handler& dh) const -> failure_or<void> {
  // Validate the tls record structure first
  TRY(validate_tls_record(dh));
  // Warn if explicit TLS options are used - they are deprecated in favor of
  // the record form
  auto warn_explicit
    = [&](const auto& opt, std::string_view name, std::string_view rec_key) {
        if (opt) {
          diagnostic::warning("`{}` is deprecated", name)
            .primary(*opt)
            .hint("set `tls.{}` instead", rec_key)
            .emit(dh);
        }
      };
  warn_explicit(skip_peer_verification_, "skip_peer_verification",
                "skip_peer_verification");
  warn_explicit(cacert_, "cacert", "cacert");
  warn_explicit(certfile_, "certfile", "certfile");
  warn_explicit(keyfile_, "keyfile", "keyfile");
  warn_explicit(password_, "password", "password");
  warn_explicit(tls_min_version_, "tls_minium_version", "min_version");
  warn_explicit(tls_ciphers_, "tls_ciphers", "ciphers");
  warn_explicit(tls_client_ca_, "tls_client_ca", "client_ca");
  warn_explicit(tls_require_client_cert_, "tls_require_client_cert",
                "require_client_cert");
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
  // Determine if TLS is enabled from the tls_ option
  // - bool false means disabled
  // - bool true or record means enabled
  const auto tls_enabled = [&]() -> std::optional<bool> {
    if (not tls_) {
      return std::nullopt; // Not explicitly set
    }
    if (const auto* b = try_as<bool>(&tls_->inner)) {
      return *b;
    }
    // Record means TLS is enabled
    if (is<record>(tls_->inner)) {
      return true;
    }
    return std::nullopt;
  }();
  if (tls_enabled.has_value()) {
    if ((url_says_safe and not *tls_enabled)
        or (url_says_unsafe and *tls_enabled)) {
      diagnostic::error("conflicting TLS settings")
        .primary(url_loc, "url {} TLS", url_says_safe ? "enables" : "disables")
        .primary(tls_->source, "option {} TLS",
                 *tls_enabled ? "enables" : "disables")
        .emit(dh);
      return failure::promise();
    }
  }
  return validate(dh);
}

auto tls_options::update_from_config(operator_control_plane& ctrl) -> void {
  // Only update tls_ from config if not explicitly set
  if (not tls_) {
    auto config_tls = get_tls(&ctrl);
    tls_ = located{data{config_tls.inner}, config_tls.source};
  }
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
    // Handle bool case
    if (const auto* b = try_as<bool>(&tls_->inner)) {
      return located{*b, tls_->source};
    }
    // Handle record case - TLS is implicitly enabled
    if (is<record>(tls_->inner)) {
      return located{true, tls_->source};
    }
    // Fallback (should not happen after validation)
    return located{true, tls_->source};
  }
  if (auto* x = query_config<bool>("tenzir.tls.enable", ctrl)) {
    return {*x, location::unknown};
  }
  return {true, location::unknown};
}

auto tls_options::get_skip_peer_verification(operator_control_plane* ctrl) const
  -> located<bool> {
  // Priority 1: Check tls record
  if (auto val = get_record_bool("skip_peer_verification")) {
    return *val;
  }
  // Priority 2: Check explicit member
  if (skip_peer_verification_) {
    return *skip_peer_verification_;
  }
  // Priority 3: Check config
  if (auto* x = query_config<bool>("tenzir.tls.skip-peer-verification", ctrl)) {
    return {*x, location::unknown};
  }
  return {false, location::unknown};
}

auto tls_options::get_cacert(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("cacert")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (cacert_) {
    return cacert_;
  }
  // Priority 3: Check config
  if (auto x = query_config<std::string>("tenzir.tls.cacert", ctrl);
      x and not x->empty()) {
    return located{*x, location::unknown};
  }
  return query_config_or_null<std::string>("tenzir.cacert", ctrl);
}

auto tls_options::get_certfile(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("certfile")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (certfile_) {
    return certfile_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.certfile", ctrl);
}

auto tls_options::get_keyfile(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("keyfile")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (keyfile_) {
    return *keyfile_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.keyfile", ctrl);
}

auto tls_options::get_password(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("password")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (password_) {
    return *password_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.password", ctrl);
}

auto tls_options::get_tls_min_version(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("min_version")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (tls_min_version_) {
    return *tls_min_version_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.tls-min-version", ctrl);
}

auto tls_options::get_tls_ciphers(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("ciphers")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (tls_ciphers_) {
    return *tls_ciphers_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.tls-ciphers", ctrl);
}

auto tls_options::get_tls_client_ca(operator_control_plane* ctrl) const
  -> std::optional<located<std::string>> {
  // Priority 1: Check tls record
  if (auto val = get_record_string("client_ca")) {
    return val;
  }
  // Priority 2: Check explicit member
  if (tls_client_ca_) {
    return *tls_client_ca_;
  }
  // Priority 3: Check config
  return query_config_or_null<std::string>("tenzir.tls.tls-client-ca", ctrl);
}

auto tls_options::get_tls_require_client_cert(operator_control_plane* ctrl) const
  -> located<bool> {
  // Priority 1: Check tls record
  if (auto val = get_record_bool("require_client_cert")) {
    return *val;
  }
  // Priority 2: Check explicit member
  if (tls_require_client_cert_) {
    return *tls_require_client_cert_;
  }
  // Priority 3: Check config
  if (auto* x = query_config<bool>("tenzir.tls.require-client-ca", ctrl)) {
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
  if (auto x = get_password(ctrl)) {
    if (auto ec = easy.set(CURLOPT_SSLKEYPASSWD, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `password`: {}", to_string(ec))
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

auto tls_options::make_caf_context(operator_control_plane& ctrl,
                                   std::optional<caf::uri> uri) const
  -> caf::expected<caf::net::ssl::context> {
  using namespace caf::net;
  auto& dh = ctrl.diagnostics();
  const auto tls_enabled
    = get_tls(&ctrl).inner or (uri and uri->scheme() == "https");
  auto min_version = ssl::tls::any;
  if (auto min = get_tls_min_version(&ctrl)) {
    if (not min->inner.empty()) {
      if (auto parsed = parse_caf_tls_version(min->inner)) {
        min_version = *parsed;
      } else {
        diagnostic::error(parsed.error()).primary(*min).emit(dh);
        return caf::make_error(ec::invalid_configuration,
                               "invalid TLS minimum version");
      }
    }
  }
  auto ctx = ssl::context::enable(tls_enabled)
               .and_then(ssl::emplace_context(min_version))
               .and_then(ssl::use_private_key_file_if(inner(get_keyfile(&ctrl)),
                                                      ssl::format::pem))
               .and_then(ssl::use_certificate_file_if(
                 inner(get_certfile(&ctrl)), ssl::format::pem))
               .and_then(ssl::use_password_if(inner(get_password(&ctrl))));
  if (uri) {
    ctx = std::move(ctx).and_then(ssl::use_sni_hostname(std::move(*uri)));
  }
  if (not ctx) {
    return ctx;
  }
  auto& concrete = *ctx;
  const auto require_client_cert = get_tls_require_client_cert(&ctrl).inner;
  const auto skip_peer_verification = get_skip_peer_verification(&ctrl).inner;
  auto verify_mode = ssl::verify::none;
  if (not skip_peer_verification or require_client_cert) {
    verify_mode |= ssl::verify::peer;
    if (require_client_cert) {
      verify_mode |= ssl::verify::fail_if_no_peer_cert;
    }
  }
  concrete.verify_mode(verify_mode);
  if (verify_mode != ssl::verify::none) {
    auto load_ca = [&](const located<std::string>& ca) -> caf::expected<void> {
      if (concrete.load_verify_file(ca.inner)) {
        return {};
      }
      diagnostic::error("failed to load TLS CA certificate")
        .primary(ca)
        .emit(dh);
      return caf::make_error(ec::invalid_configuration,
                             "failed to load TLS CA certificate");
    };
    if (require_client_cert) {
      if (auto client_ca = get_tls_client_ca(&ctrl)) {
        if (auto res = load_ca(*client_ca); not res) {
          return caf::make_error(ec::invalid_configuration,
                                 "failed to configure TLS client CA");
        }
      }
    }
    if (auto cacert = get_cacert(&ctrl)) {
      if (auto res = load_ca(*cacert); not res) {
        return caf::make_error(ec::invalid_configuration,
                               "failed to configure TLS CA");
      }
    } else if (not concrete.enable_default_verify_paths()) {
      return caf::make_error(ec::invalid_configuration,
                             "failed to enable default verify paths");
    }
  }
  if (auto ciphers = get_tls_ciphers(&ctrl)) {
    if (auto* native = static_cast<SSL_CTX*>(concrete.native_handle())) {
      if (SSL_CTX_set_cipher_list(native, ciphers->inner.c_str()) != 1) {
        diagnostic::warning("failed to set TLS cipher list")
          .primary(*ciphers)
          .emit(dh);
      }
    }
  }
  return ctx;
}

} // namespace tenzir
