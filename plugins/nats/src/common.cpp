//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir_plugins/nats/common.hpp"

#include <tenzir/async.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/try.hpp>

#include <caf/error.hpp>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <span>

namespace tenzir::plugins::nats {

namespace {

constexpr auto default_url_value = std::string_view{"nats://localhost:4222"};

auto get_plugins_nats_config(record const& global_config) -> record const* {
  auto plugins_it = global_config.find("plugins");
  if (plugins_it == global_config.end()) {
    return nullptr;
  }
  auto const* plugins = try_as<record>(&plugins_it->second);
  if (not plugins) {
    return nullptr;
  }
  auto nats_it = plugins->find("nats");
  if (nats_it == plugins->end()) {
    return nullptr;
  }
  return try_as<record>(&nats_it->second);
}

auto set_opt_secret_value(record const& config, std::string_view key,
                          secret& out, std::string_view plugin_name)
  -> caf::error {
  auto it = config.find(std::string{key});
  if (it == config.end()) {
    return caf::none;
  }
  return match(
    it->second,
    [&](std::string const& value) -> caf::error {
      out = secret::make_literal(value);
      return caf::none;
    },
    [&](secret const& value) -> caf::error {
      out = value;
      return caf::none;
    },
    [&](auto const&) -> caf::error {
      return diagnostic::error("`plugins.nats.{}` must be string or secret",
                               key)
        .note("while initializing `{}`", plugin_name)
        .to_error();
    });
}

auto auth_record_keys() -> std::span<const std::string_view> {
  static constexpr auto keys = std::array{
    std::string_view{"user"},  std::string_view{"password"},
    std::string_view{"token"}, std::string_view{"credentials"},
    std::string_view{"seed"},  std::string_view{"credentials_memory"},
  };
  return keys;
}

auto is_string_or_secret(data const& value) -> bool {
  return is<std::string>(value) or is<secret>(value);
}

auto check(natsStatus status, diagnostic_builder diag, diagnostic_handler& dh)
  -> failure_or<void> {
  if (status == NATS_OK) {
    return {};
  }
  emit_nats_error(std::move(diag), status, dh);
  return failure::promise();
}

auto set_auth_value(auth_config& auth, std::string const& key,
                    std::string value) -> void {
  if (key == "user") {
    auth.user = std::move(value);
  } else if (key == "password") {
    auth.password = std::move(value);
  } else if (key == "token") {
    auth.token = std::move(value);
  } else if (key == "credentials") {
    auth.credentials = std::move(value);
  } else if (key == "seed") {
    auth.seed = std::move(value);
  } else if (key == "credentials_memory") {
    auth.credentials_memory = std::move(value);
  }
}

} // namespace

auto default_url() -> secret& {
  static auto result = secret::make_literal(default_url_value);
  return result;
}

auto set_default_url_from_global_config(record const& global_config,
                                        std::string_view plugin_name)
  -> caf::error {
  default_url() = secret::make_literal(default_url_value);
  auto const* config = get_plugins_nats_config(global_config);
  if (not config) {
    return caf::none;
  }
  return set_opt_secret_value(*config, "url", default_url(), plugin_name);
}

auto has_url_scheme(std::string_view url) -> bool {
  return url.find("://") != std::string_view::npos;
}

auto tls_enabled_from_url(std::string_view url) -> bool {
  return url.starts_with("tls://") or url.starts_with("wss://");
}

auto normalize_url(std::string url, bool tls_enabled) -> std::string {
  if (not has_url_scheme(url)) {
    url.insert(0, tls_enabled ? "tls://" : "nats://");
  }
  return url;
}

auto nats_status_string(natsStatus status) -> std::string {
  auto const* text = natsStatus_GetText(status);
  if (text == nullptr) {
    return fmt::format("NATS status {}", static_cast<int>(status));
  }
  return text;
}

auto nats_last_error_string(natsStatus fallback) -> std::string {
  auto last_status = NATS_OK;
  auto const* text = nats_GetLastError(&last_status);
  if (text and *text) {
    return fmt::format("{}: {}", nats_status_string(last_status), text);
  }
  return nats_status_string(fallback);
}

auto emit_nats_error(diagnostic_builder diag, natsStatus status,
                     diagnostic_handler& dh) -> void {
  std::move(diag).note("reason: {}", nats_last_error_string(status)).emit(dh);
}

auto validate_auth_record(Option<located<data>> const& auth,
                          diagnostic_handler& dh) -> failure_or<void> {
  if (not auth) {
    return {};
  }
  auto const* rec = try_as<record>(&auth->inner);
  if (not rec) {
    diagnostic::error("`auth` must be a record").primary(auth->source).emit(dh);
    return failure::promise();
  }
  for (auto const& [key, value] : *rec) {
    auto valid_key
      = std::ranges::find(auth_record_keys(), key) != auth_record_keys().end();
    if (not valid_key) {
      diagnostic::error("unknown key `{}` in `auth` record", key)
        .primary(auth->source)
        .hint("valid keys are: {}", fmt::join(auth_record_keys(), ", "))
        .emit(dh);
      return failure::promise();
    }
    if (not is_string_or_secret(value)) {
      diagnostic::error("`auth.{}` must be string or secret", key)
        .primary(auth->source)
        .emit(dh);
      return failure::promise();
    }
  }
  auto has = [&](std::string_view key) {
    return rec->find(std::string{key}) != rec->end();
  };
  if (has("token")
      and (has("user") or has("password") or has("credentials")
           or has("credentials_memory") or has("seed"))) {
    diagnostic::error("`auth.token` cannot be combined with other auth modes")
      .primary(auth->source)
      .emit(dh);
    return failure::promise();
  }
  if (has("credentials_memory")
      and (has("credentials") or has("seed") or has("user")
           or has("password"))) {
    diagnostic::error(
      "`auth.credentials_memory` cannot be combined with other auth modes")
      .primary(auth->source)
      .emit(dh);
    return failure::promise();
  }
  if ((has("credentials") or has("seed"))
      and (has("user") or has("password"))) {
    diagnostic::error(
      "`auth.credentials` cannot be combined with user/password")
      .primary(auth->source)
      .emit(dh);
    return failure::promise();
  }
  if (has("password") and not has("user")) {
    diagnostic::error("`auth.password` requires `auth.user`")
      .primary(auth->source)
      .emit(dh);
    return failure::promise();
  }
  if (has("seed") and not has("credentials")) {
    diagnostic::error("`auth.seed` requires `auth.credentials`")
      .primary(auth->source)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto resolve_connection_config(OpCtx& ctx, Option<located<secret>> const& url,
                               Option<located<data>> const& auth)
  -> Task<Option<connection_config>> {
  auto result = connection_config{};
  auto requests = std::vector<secret_request>{};
  auto const& url_secret = url ? url->inner : default_url();
  auto const url_loc = url ? url->source : location::unknown;
  requests.push_back(
    make_secret_request("url", url_secret, url_loc, result.url, ctx.dh()));
  if (auth) {
    if (auto const* auth_record = try_as<record>(&auth->inner)) {
      for (auto const& [key, value] : *auth_record) {
        match(
          value,
          [&](std::string const& str) {
            set_auth_value(result.auth, key, str);
          },
          [&](secret const& sec) {
            requests.emplace_back(
              sec, auth->source,
              [&result, key, loc = auth->source, &dh = ctx.dh()](
                resolved_secret_value value) -> failure_or<void> {
                TRY(auto view, value.utf8_view(key, loc, dh));
                set_auth_value(result.auth, key, std::string{view});
                return {};
              });
          },
          [](auto const&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
  }
  if (auto ok = co_await ctx.resolve_secrets(std::move(requests));
      ok.is_error()) {
    co_return None{};
  }
  if (result.url.empty()) {
    diagnostic::error("`url` must not be empty").primary(url_loc).emit(ctx);
    co_return None{};
  }
  co_return result;
}

auto apply_auth(natsOptions* options, auth_config const& auth,
                location auth_location, diagnostic_handler& dh)
  -> failure_or<void> {
  if (auth.token) {
    TRY(check(natsOptions_SetToken(options, auth.token->c_str()),
              diagnostic::error("failed to configure NATS token auth")
                .primary(auth_location),
              dh));
  }
  if (auth.user) {
    auto const* password = auth.password ? auth.password->c_str() : "";
    TRY(check(natsOptions_SetUserInfo(options, auth.user->c_str(), password),
              diagnostic::error("failed to configure NATS user/password auth")
                .primary(auth_location),
              dh));
  }
  if (auth.credentials_memory) {
    TRY(check(natsOptions_SetUserCredentialsFromMemory(
                options, auth.credentials_memory->c_str()),
              diagnostic::error("failed to configure NATS credentials")
                .primary(auth_location),
              dh));
  }
  if (auth.credentials) {
    auto const* seed = auth.seed ? auth.seed->c_str() : nullptr;
    TRY(check(natsOptions_SetUserCredentialsFromFiles(
                options, auth.credentials->c_str(), seed),
              diagnostic::error("failed to configure NATS credentials files")
                .primary(auth_location),
              dh));
  }
  return {};
}

auto apply_tls(natsOptions* options, tls_options const& tls,
               diagnostic_handler& dh) -> failure_or<void> {
  auto enabled = tls.get_tls(nullptr);
  TRY(check(
    natsOptions_SetSecure(options, enabled.inner),
    diagnostic::error("failed to configure NATS TLS").primary(enabled.source),
    dh));
  if (not enabled.inner) {
    return {};
  }
  if (auto cacert = tls.get_cacert(nullptr)) {
    TRY(check(natsOptions_LoadCATrustedCertificates(options,
                                                    cacert->inner.c_str()),
              diagnostic::error("failed to load NATS TLS CA certificate")
                .primary(cacert->source),
              dh));
  }
  auto certfile = tls.get_certfile(nullptr);
  auto keyfile = tls.get_keyfile(nullptr);
  if (certfile or keyfile) {
    if (not certfile) {
      diagnostic::error("`tls.certfile` is required when `tls.keyfile` is set")
        .primary(keyfile->source)
        .hint("set both `tls.certfile` and `tls.keyfile`")
        .emit(dh);
      return failure::promise();
    }
    auto const* key
      = keyfile ? keyfile->inner.c_str() : certfile->inner.c_str();
    TRY(check(natsOptions_LoadCertificatesChain(options,
                                                certfile->inner.c_str(), key),
              diagnostic::error("failed to load NATS TLS client certificate")
                .primary(certfile->source),
              dh));
  }
  if (auto ciphers = tls.get_tls_ciphers(nullptr)) {
    TRY(check(natsOptions_SetCiphers(options, ciphers->inner.c_str()),
              diagnostic::error("failed to configure NATS TLS ciphers")
                .primary(ciphers->source),
              dh));
  }
  auto skip = tls.get_skip_peer_verification(nullptr);
  TRY(check(natsOptions_SkipServerVerification(options, skip.inner),
            diagnostic::error("failed to configure NATS TLS verification")
              .primary(skip.source),
            dh));
  if (auto min_version = tls.get_tls_min_version(nullptr)) {
    diagnostic::warning("`tls.min_version` is not supported by nats.c")
      .primary(min_version->source)
      .note("the option is ignored for NATS connections")
      .emit(dh);
  }
  return {};
}

auto make_nats_options(connection_config const& config,
                       Option<located<data>> const& tls_arg,
                       location url_location, diagnostic_handler& dh,
                       folly::EventBase& event_base)
  -> failure_or<nats_options_ptr> {
  auto* raw_options = static_cast<natsOptions*>(nullptr);
  TRY(check(
    natsOptions_Create(&raw_options),
    diagnostic::error("failed to create NATS options").primary(url_location),
    dh));
  auto options = nats_options_ptr{raw_options};
  auto tls
    = tls_arg ? tls_options{*tls_arg, {.tls_default = true, .is_server = false}}
              : tls_options{tls_options::options{
                  .tls_default = tls_enabled_from_url(config.url),
                  .is_server = false,
                }};
  TRY(apply_tls(options.get(), tls, dh));
  auto const normalized_url
    = normalize_url(config.url, tls.get_tls(nullptr).inner);
  TRY(check(
    natsOptions_SetURL(options.get(), normalized_url.c_str()),
    diagnostic::error("failed to configure NATS URL").primary(url_location),
    dh));
  TRY(check(configure_folly_event_loop(options.get(), event_base),
            diagnostic::error("failed to configure NATS event loop")
              .primary(url_location),
            dh));
  TRY(check(natsOptions_SetName(options.get(), "tenzir"),
            diagnostic::error("failed to configure NATS client name")
              .primary(url_location),
            dh));
  TRY(apply_auth(options.get(), config.auth, url_location, dh));
  return options;
}

} // namespace tenzir::plugins::nats
