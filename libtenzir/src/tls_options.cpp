//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tls_options.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/operator_control_plane.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/net/ssl/context.hpp>
#include <curl/curl.h>
#include <folly/io/async/SSLContext.h>
#include <folly/ssl/PasswordCollector.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <memory>

namespace tenzir {

tls_options::tls_options(located<data> tls_val) : tls_{std::move(tls_val)} {
}

tls_options::tls_options(located<data> tls_val, options opts)
  : uses_curl_http_{opts.uses_curl_http},
    is_server_{opts.is_server},
    tls_{std::move(tls_val)} {
}

namespace {

// Valid keys for the tls record (snake_case, no tls_ prefix)
constexpr std::array<std::string_view, 9> valid_tls_record_keys = {
  "skip_peer_verification",
  "cacert",
  "certfile",
  "keyfile",
  "password",
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

// A password collector that hands OpenSSL an in-memory password instead of
// reading it from a file. folly only ships `PasswordInFile`, but our `password`
// option carries the secret inline.
class inline_password_collector final : public folly::ssl::PasswordCollector {
public:
  explicit inline_password_collector(std::string password)
    : password_{std::move(password)} {
  }

  void getPassword(std::string& password, int /*size*/) const override {
    password = password_;
  }

  auto describe() const -> const std::string& override {
    static const auto description = std::string{"inline TLS key password"};
    return description;
  }

private:
  std::string password_;
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

auto resolve_regular_file(located<std::string> const& value,
                          std::string_view key, diagnostic_handler& dh)
  -> failure_or<std::filesystem::path> {
  auto ec = std::error_code{};
  auto path = std::filesystem::canonical(value.inner, ec);
  if (ec or not std::filesystem::is_regular_file(path, ec)) {
    diagnostic::error("`{}` path is not a valid file", key)
      .primary(value)
      .emit(dh);
    return failure::promise();
  }
  return path;
}

template <typename T>
auto query_config(std::string_view name, const caf::actor_system_config& cfg)
  -> const T* {
  return caf::get_if<T>(&cfg.content, name);
}

template <typename T>
auto query_config_or_null(std::string_view name,
                          const caf::actor_system_config& cfg)
  -> std::optional<located<T>> {
  if (auto* x = query_config<T>(name, cfg)) {
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

template <typename T>
constexpr auto inner(const Option<located<T>>& x) -> std::optional<T> {
  if (not x) {
    return std::nullopt;
  }
  return x->inner;
}

} // namespace

auto add_tls_client_diagnostic_hints(diagnostic_builder diag, bool tls_enabled,
                                     std::string_view service_name,
                                     std::optional<uint64_t> plaintext_port,
                                     std::optional<uint64_t> tls_port)
  -> diagnostic_builder {
  diag = std::move(diag).note("`tls` is {} for this connection",
                              tls_enabled ? "enabled" : "disabled");
  if (not service_name.empty() and plaintext_port and tls_port) {
    diag = std::move(diag).note("common {} ports are `{}` without TLS and `{}` "
                                "with TLS",
                                service_name, *plaintext_port, *tls_port);
  }
  if (tls_enabled) {
    if (service_name.empty()) {
      return std::move(diag).hint(
        "if the server expects a plaintext connection, set `tls=false`");
    }
    return std::move(diag).hint("if the {} server expects a plaintext "
                                "connection, set `tls=false`",
                                service_name);
  }
  if (service_name.empty()) {
    return std::move(diag).hint("if the server requires TLS, set `tls=true`");
  }
  return std::move(diag).hint("if the {} server requires TLS, set `tls=true`",
                              service_name);
}

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
  // Validate file paths exist.
  auto check_file = [&](std::string_view key) -> failure_or<void> {
    auto it = rec->find(key);
    if (it == rec->end()) {
      return {};
    }
    auto const* sval = try_as<std::string>(&it->second);
    if (not sval) {
      return {};
    }
    auto ec = std::error_code{};
    auto path = std::filesystem::canonical(*sval, ec);
    if (ec or not std::filesystem::is_regular_file(path, ec)) {
      diagnostic::error("`tls.{}` path is not a valid file: {}", key, *sval)
        .primary(tls_->source)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(check_file("cacert"));
  TRY(check_file("certfile"));
  TRY(check_file("keyfile"));
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
  if (is_server_ and get_tls().inner) {
    // `tls=true` may rely on defaults from config. Only reject incomplete
    // explicit key-pair configuration if a key file is set without a cert file.
    auto has_certfile
      = get_record_string("certfile").has_value() or certfile_.has_value();
    auto has_keyfile
      = get_record_string("keyfile").has_value() or keyfile_.has_value();
    auto tls_loc = tls_.has_value() ? tls_->source : location::unknown;
    if (has_keyfile and not has_certfile) {
      diagnostic::error("`tls.certfile` is required when `tls.keyfile` is set")
        .primary(tls_loc)
        .hint("set both `tls.certfile` and `tls.keyfile`")
        .emit(dh);
      return failure::promise();
    }
  }
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
  if (get_tls().inner and not get_skip_peer_verification().inner) {
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
  // - location::unknown means default/config-derived, not explicitly user-set
  const auto tls_enabled = [&]() -> std::optional<bool> {
    if (not tls_) {
      return std::nullopt; // Not explicitly set
    }
    if (tls_->source == location::unknown) {
      return std::nullopt; // Default or config-derived, not user-provided
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

auto tls_options::apply_config(const caf::actor_system_config& cfg) -> void {
  // Merge node-config defaults into the cached members. Values explicitly set
  // on the operator (via `tls_` or a legacy top-level option) win and are not
  // overwritten -- this is what the per-getter precedence below encodes.
  if (not tls_) {
    if (auto* x = query_config<bool>("tenzir.tls.enable", cfg)) {
      tls_ = located{data{*x}, location::unknown};
    }
  }
  auto merge_bool
    = [&](std::optional<located<bool>>& slot, std::string_view key) {
        if (not slot) {
          if (auto* x = query_config<bool>(key, cfg)) {
            slot = located{*x, location::unknown};
          }
        }
      };
  auto merge_string
    = [&](std::optional<located<std::string>>& slot, std::string_view key) {
        if (not slot) {
          if (auto x = query_config_or_null<std::string>(key, cfg)) {
            slot = std::move(*x);
          }
        }
      };
  merge_bool(skip_peer_verification_, "tenzir.tls.skip-peer-verification");
  // `tenzir.tls.cacert` takes precedence over the legacy `tenzir.cacert`.
  if (not cacert_) {
    if (auto x = query_config<std::string>("tenzir.tls.cacert", cfg);
        x and not x->empty()) {
      cacert_ = located{*x, location::unknown};
    } else if (auto fallback
               = query_config_or_null<std::string>("tenzir.cacert", cfg)) {
      cacert_ = std::move(*fallback);
    }
  }
  merge_string(certfile_, "tenzir.tls.certfile");
  merge_string(keyfile_, "tenzir.tls.keyfile");
  merge_string(password_, "tenzir.tls.password");
  merge_string(tls_min_version_, "tenzir.tls.tls-min-version");
  merge_string(tls_ciphers_, "tenzir.tls.tls-ciphers");
  merge_string(tls_client_ca_, "tenzir.tls.tls-client-ca");
  merge_bool(tls_require_client_cert_, "tenzir.tls.require-client-cert");
}

// Each getter returns the effective value, considering (in order):
//   1. The `tls` record entry on the operator (highest priority).
//   2. The legacy top-level option (e.g. `cacert=...`) on the operator.
//   3. Node-config defaults previously merged in via `apply_config()`.
// Step 3 is invisible here: it works by `apply_config()` writing the
// resolved defaults into the same member variables that step 2 reads.

auto tls_options::get_tls() const -> located<bool> {
  if (tls_) {
    if (const auto* b = try_as<bool>(&tls_->inner)) {
      return located{*b, tls_->source};
    }
    // Record means TLS is implicitly enabled. The fallback case below should
    // not happen after validation.
    return located{true, tls_->source};
  }
  return {true, location::unknown};
}

auto tls_options::get_skip_peer_verification() const -> located<bool> {
  if (auto val = get_record_bool("skip_peer_verification")) {
    return *val;
  }
  if (skip_peer_verification_) {
    return *skip_peer_verification_;
  }
  return {false, location::unknown};
}

auto tls_options::get_cacert() const -> std::optional<located<std::string>> {
  if (auto val = get_record_string("cacert")) {
    return val;
  }
  return cacert_;
}

auto tls_options::get_certfile() const -> std::optional<located<std::string>> {
  if (auto val = get_record_string("certfile")) {
    return val;
  }
  return certfile_;
}

auto tls_options::get_keyfile() const -> std::optional<located<std::string>> {
  if (auto val = get_record_string("keyfile")) {
    return val;
  }
  return keyfile_;
}

auto tls_options::get_password() const -> std::optional<located<std::string>> {
  if (auto val = get_record_string("password")) {
    return val;
  }
  return password_;
}

auto tls_options::get_tls_min_version() const
  -> std::optional<located<std::string>> {
  if (auto val = get_record_string("min_version")) {
    return val;
  }
  return tls_min_version_;
}

auto tls_options::get_tls_ciphers() const
  -> std::optional<located<std::string>> {
  if (auto val = get_record_string("ciphers")) {
    return val;
  }
  return tls_ciphers_;
}

auto tls_options::get_tls_client_ca() const
  -> std::optional<located<std::string>> {
  if (auto val = get_record_string("client_ca")) {
    return val;
  }
  return tls_client_ca_;
}

auto tls_options::get_tls_require_client_cert() const -> located<bool> {
  if (auto val = get_record_bool("require_client_cert")) {
    return *val;
  }
  if (tls_require_client_cert_) {
    return *tls_require_client_cert_;
  }
  return {false, location::unknown};
}

// -- TlsConfig ---------------------------------------------------------------
//
// All runtime TLS operations live here. The corresponding `tls_options`
// methods used to exist as parallel implementations; they were deleted once
// every call site had migrated to `tls_options::resolve()` + `TlsConfig`.

auto TlsConfig::update_url(std::string_view url) const -> std::string {
  auto url_copy = std::string{url};
  if (not uses_curl_http) {
    return url_copy;
  }
  if (not tls.inner) {
    return url_copy;
  }
  // If the URL says http, and the TLS option was not defaulted.
  if (url.starts_with("http://") and tls.source != location::unknown) {
    url_copy.insert(4, "s");
  }
  return url_copy;
}

auto TlsConfig::apply_to(curl::easy& easy, std::string_view url) const
  -> caf::error {
  auto used_url = update_url(url);
  check(easy.set(CURLOPT_URL, used_url));
  if (tls.inner) {
    check(easy.set(CURLOPT_DEFAULT_PROTOCOL, "https"));
  }
  if (auto& x = cacert) {
    if (auto ec = easy.set(CURLOPT_CAINFO, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `cacert`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  if (auto& x = certfile) {
    if (auto ec = easy.set(CURLOPT_SSLCERT, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `certfile`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  if (auto& x = keyfile) {
    if (auto ec = easy.set(CURLOPT_SSLKEY, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `keyfile`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  if (auto& x = password) {
    if (auto ec = easy.set(CURLOPT_SSLKEYPASSWD, x->inner);
        ec != curl::easy::code::ok) {
      return diagnostic::error("failed to set `password`: {}", to_string(ec))
        .primary(*x)
        .to_error();
    }
  }
  check(
    easy.set(CURLOPT_USE_SSL, tls.inner ? CURLUSESSL_ALL : CURLUSESSL_NONE));
  check(easy.set(CURLOPT_SSL_VERIFYPEER, skip_peer_verification.inner ? 0 : 1));
  check(easy.set(CURLOPT_SSL_VERIFYHOST, skip_peer_verification.inner ? 0 : 1));
  if (auto& x = tls_min_version) {
    auto curl_version = parse_curl_tls_version(x->inner);
    if (curl_version) {
      check(easy.set(CURLOPT_SSLVERSION, *curl_version));
    } else {
      return diagnostic::error(curl_version.error()).primary(*x).to_error();
    }
  }
  if (auto& x = tls_ciphers) {
    check(easy.set(CURLOPT_SSL_CIPHER_LIST, x->inner));
  }
  return {};
}

auto TlsConfig::make_caf_context(operator_control_plane& ctrl,
                                 std::optional<caf::uri> uri) const
  -> caf::expected<caf::net::ssl::context> {
  using namespace caf::net;
  auto& dh = ctrl.diagnostics();
  const auto tls_enabled = tls.inner or (uri and uri->scheme() == "https");
  auto min_version = ssl::tls::any;
  if (auto& min = tls_min_version) {
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
               .and_then(
                 ssl::use_private_key_file_if(inner(keyfile), ssl::format::pem))
               .and_then(ssl::use_certificate_file_if(inner(certfile),
                                                      ssl::format::pem))
               .and_then(ssl::use_password_if(inner(password)));
  if (uri) {
    ctx = std::move(ctx).and_then(ssl::use_sni_hostname(std::move(*uri)));
  }
  if (not ctx) {
    return ctx;
  }
  auto& concrete = *ctx;
  const auto require_cert = tls_require_client_cert.inner;
  const auto skip_verify = skip_peer_verification.inner;
  auto verify_mode = ssl::verify::none;
  if (not skip_verify or require_cert) {
    verify_mode |= ssl::verify::peer;
    if (require_cert) {
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
    if (require_cert) {
      if (auto& client_ca = tls_client_ca) {
        if (auto res = load_ca(*client_ca); not res) {
          return caf::make_error(ec::invalid_configuration,
                                 "failed to configure TLS client CA");
        }
      }
    }
    if (auto& ca = cacert) {
      if (auto res = load_ca(*ca); not res) {
        return caf::make_error(ec::invalid_configuration,
                               "failed to configure TLS CA");
      }
    } else if (not concrete.enable_default_verify_paths()) {
      return caf::make_error(ec::invalid_configuration,
                             "failed to enable default verify paths");
    }
  }
  if (auto& ciphers = tls_ciphers) {
    auto cipher_loc = ciphers->source;
    if (cipher_loc == tls_arg_source) {
      // `located<data>` for `tls={...}` only carries the whole record span.
      // Clamp to a tiny span to avoid misleading multi-line highlights.
      cipher_loc = cipher_loc.subloc(0, 1);
    }
    if (auto* native = static_cast<SSL_CTX*>(concrete.native_handle())) {
      if (SSL_CTX_set_cipher_list(native, ciphers->inner.c_str()) != 1) {
        diagnostic::error("invalid TLS cipher list")
          .primary(cipher_loc, "`tls.ciphers`")
          .emit(dh);
        return caf::make_error(ec::invalid_configuration,
                               "invalid TLS cipher list");
      }
    }
  }
  return ctx;
}

auto TlsConfig::make_folly_ssl_context(diagnostic_handler& dh,
                                       bool tls_required) const
  -> failure_or<std::shared_ptr<folly::SSLContext>> {
  if (not tls.inner and not tls_required) {
    return nullptr;
  }
  auto ctx = std::make_shared<folly::SSLContext>(folly::SSLContext::TLSv1_2);
  auto skip_verify = skip_peer_verification.inner;
  auto require_cert = tls_require_client_cert.inner;
  // Apply minimum TLS version.
  if (auto& min = tls_min_version) {
    if (not min->inner.empty()) {
      auto parsed = parse_openssl_tls_version(min->inner);
      if (not parsed) {
        diagnostic::error(parsed.error()).primary(*min).emit(dh);
        return failure::promise();
      }
      SSL_CTX_set_min_proto_version(ctx->getSSLCtx(), *parsed);
    }
  }
  auto should_verify_peer = not skip_verify or require_cert;
  // Load CA certificate only when peer verification is enabled.
  if (should_verify_peer and cacert) {
    TRY(auto path, resolve_regular_file(*cacert, "cacert", dh));
    try {
      ctx->loadTrustedCertificates(path.c_str());
    } catch (std::exception const& ex) {
      diagnostic::error("failed to load CA certificate: {}", ex.what())
        .primary(*cacert)
        .emit(dh);
      return failure::promise();
    }
  }
  // When acting as a server that requires client certificates, load the client
  // CA so presented certificates can be validated, and advertise it in the
  // `CertificateRequest` sent to clients. This mirrors the asio and http_server
  // paths; without it, `require-client-cert` is silently not enforced.
  if (require_cert and tls_client_ca) {
    TRY(auto path, resolve_regular_file(*tls_client_ca, "client_ca", dh));
    try {
      ctx->loadTrustedCertificates(path.c_str());
    } catch (std::exception const& ex) {
      diagnostic::error("failed to load client CA certificate: {}", ex.what())
        .primary(*tls_client_ca)
        .emit(dh);
      return failure::promise();
    }
    if (auto* names = SSL_load_client_CA_file(path.c_str())) {
      SSL_CTX_set_client_CA_list(ctx->getSSLCtx(), names);
    }
  }
  // Load certificate chain.
  if (certfile) {
    TRY(auto path, resolve_regular_file(*certfile, "certfile", dh));
    try {
      ctx->loadCertificate(path.c_str());
    } catch (std::exception const& ex) {
      diagnostic::error("failed to load client certificate: {}", ex.what())
        .primary(*certfile)
        .emit(dh);
      return failure::promise();
    }
  }
  // Apply the private key password, if any, before loading the key. OpenSSL
  // queries this collector when the key file is encrypted.
  if (password) {
    ctx->passwordCollector(
      std::make_shared<inline_password_collector>(password->inner));
  }
  // Load private key. If `keyfile` is omitted, try reading it from `certfile`.
  if (auto& private_key_file = keyfile ? keyfile : certfile) {
    const auto* key_name = keyfile ? "keyfile" : "certfile";
    TRY(auto path, resolve_regular_file(*private_key_file, key_name, dh));
    try {
      ctx->loadPrivateKey(path.c_str());
    } catch (std::exception const& ex) {
      if (not keyfile and certfile) {
        diagnostic::error("failed to load client private key: {}", ex.what())
          .primary(*private_key_file)
          .hint("set `tls.keyfile` or include a private key in `tls.certfile`")
          .emit(dh);
      } else {
        diagnostic::error("failed to load client private key: {}", ex.what())
          .primary(*private_key_file)
          .emit(dh);
      }
      return failure::promise();
    }
  }
  if (auto& ciphers = tls_ciphers) {
    auto cipher_loc = ciphers->source;
    if (cipher_loc == tls_arg_source) {
      // See note in `make_caf_context`.
      cipher_loc = cipher_loc.subloc(0, 1);
    }
    if (SSL_CTX_set_cipher_list(ctx->getSSLCtx(), ciphers->inner.c_str())
        != 1) {
      diagnostic::error("invalid TLS cipher list")
        .primary(cipher_loc, "`tls.ciphers`")
        .emit(dh);
      return failure::promise();
    }
  }
  // Set verification mode. Requiring a client certificate takes precedence over
  // `skip_peer_verification`: a server that demands mTLS must reject clients
  // that present no certificate (`SSL_VERIFY_FAIL_IF_NO_PEER_CERT`).
  if (require_cert) {
    ctx->setVerificationOption(
      folly::SSLContext::SSLVerifyPeerEnum::VERIFY_REQ_CLIENT_CERT);
    if (not cacert and not tls_client_ca
        and SSL_CTX_set_default_verify_paths(ctx->getSSLCtx()) != 1) {
      diagnostic::error("failed to enable default verify paths").emit(dh);
      return failure::promise();
    }
  } else if (skip_verify) {
    ctx->setVerificationOption(folly::SSLContext::SSLVerifyPeerEnum::NO_VERIFY);
  } else {
    ctx->setVerificationOption(folly::SSLContext::SSLVerifyPeerEnum::VERIFY);
    if (not cacert
        and SSL_CTX_set_default_verify_paths(ctx->getSSLCtx()) != 1) {
      diagnostic::error("failed to enable default verify paths").emit(dh);
      return failure::promise();
    }
  }
  return ctx;
}

// -- tls_options::resolve ----------------------------------------------------

auto tls_options::resolve(const caf::actor_system_config& cfg,
                          diagnostic_handler& dh) const
  -> failure_or<TlsConfig> {
  TRY(validate(dh));
  // Work on a copy so resolve() is `const` on the operator's stored options.
  auto merged = *this;
  merged.apply_config(cfg);
  auto to_option =
    [](std::optional<located<std::string>> x) -> Option<located<std::string>> {
    if (x) {
      return Option<located<std::string>>{std::move(*x)};
    }
    return None{};
  };
  // `TlsConfig` has a private default constructor; we are a friend, so we
  // can construct one here. Designated initializers would require it to be
  // an aggregate, which would defeat the gating.
  auto out = TlsConfig{};
  out.tls = merged.get_tls();
  out.skip_peer_verification = merged.get_skip_peer_verification();
  out.cacert = to_option(merged.get_cacert());
  out.certfile = to_option(merged.get_certfile());
  out.keyfile = to_option(merged.get_keyfile());
  out.password = to_option(merged.get_password());
  out.tls_min_version = to_option(merged.get_tls_min_version());
  out.tls_ciphers = to_option(merged.get_tls_ciphers());
  out.tls_client_ca = to_option(merged.get_tls_client_ca());
  out.tls_require_client_cert = merged.get_tls_require_client_cert();
  out.uses_curl_http = uses_curl_http_;
  out.tls_arg_source = tls_ ? tls_->source : location::unknown;
  return out;
}

auto tls_options::resolve(std::string_view url, location url_loc,
                          const caf::actor_system_config& cfg,
                          diagnostic_handler& dh) const
  -> failure_or<TlsConfig> {
  // Same as the (cfg, dh) overload but performs URL/TLS-scheme consistency
  // checking instead of plain validation. `validate(url, ...)` calls
  // `validate(dh)` internally, so the deprecation warnings are still emitted
  // exactly once (allowing callers to use this overload instead of an
  // explicit `validate(url, ...)` + `resolve(cfg, dh)` pair, which would
  // emit them twice).
  TRY(validate(url, url_loc, dh));
  auto merged = *this;
  merged.apply_config(cfg);
  auto to_option =
    [](std::optional<located<std::string>> x) -> Option<located<std::string>> {
    if (x) {
      return Option<located<std::string>>{std::move(*x)};
    }
    return None{};
  };
  auto out = TlsConfig{};
  out.tls = merged.get_tls();
  out.skip_peer_verification = merged.get_skip_peer_verification();
  out.cacert = to_option(merged.get_cacert());
  out.certfile = to_option(merged.get_certfile());
  out.keyfile = to_option(merged.get_keyfile());
  out.password = to_option(merged.get_password());
  out.tls_min_version = to_option(merged.get_tls_min_version());
  out.tls_ciphers = to_option(merged.get_tls_ciphers());
  out.tls_client_ca = to_option(merged.get_tls_client_ca());
  out.tls_require_client_cert = merged.get_tls_require_client_cert();
  out.uses_curl_http = uses_curl_http_;
  out.tls_arg_source = tls_ ? tls_->source : location::unknown;
  return out;
}

auto tls_options::resolve(operator_control_plane& ctrl) const
  -> failure_or<TlsConfig> {
  return resolve(ctrl.self().system().config(), ctrl.diagnostics());
}

auto TlsConfig::defaults() -> TlsConfig {
  auto out = TlsConfig{};
  out.tls = located{true, location::unknown};
  out.skip_peer_verification = located{false, location::unknown};
  out.tls_require_client_cert = located{false, location::unknown};
  out.uses_curl_http = false;
  out.tls_arg_source = location::unknown;
  return out;
}

} // namespace tenzir
