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

#include <filesystem>

namespace tenzir {

ssl_options::ssl_options() = default;

auto ssl_options::add_tls_options(argument_parser2& parser) -> void {
  parser.named_optional("tls", tls)
    .named("skip_peer_verification", skip_peer_verification)
    .named("skip_host_verification", skip_hostname_verification)
    .named("cacert", cacert)
    .named("certfile", certfile)
    .named("keyfile", keyfile);
}

auto ssl_options::validate(const located<std::string>& url,
                           diagnostic_handler& dh) -> failure_or<void> {
  auto url_says_safe = url.inner.starts_with("https://")
                       or url.inner.starts_with("ftps://")
                       or url.inner.starts_with("smtps://");
  auto option_says_no_tls = not tls.inner;
  if (url_says_safe and option_says_no_tls) {
    diagnostic::error("conflicting TLS settings")
      .primary(url, "url specifies TLS")
      .primary(tls, "option specifies no TLS")
      .emit(dh);
    return failure::promise();
  }
  const auto tls_logic
    = [&](auto& thing, std::string_view name) -> failure_or<void> {
    if (tls.inner and thing) {
      diagnostic::error("`{}` requires TLS", name)
        .primary(tls.source, "TLS is disabled")
        .primary(*thing)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(tls_logic(skip_peer_verification, "skip_peer_verification"));
  TRY(tls_logic(skip_hostname_verification, "skip_hostname_verification"));
  TRY(tls_logic(cacert, "cacert"));
  TRY(tls_logic(certfile, "certfile"));
  TRY(tls_logic(keyfile, "keyfile"));
  if (tls.inner and not skip_peer_verification) {
    if (!std::filesystem::exists(cacert->inner)) {
      diagnostic::error("the configured CA certificate bundle does not exist")
        .note("configured location: {}", cacert->inner)
        .primary(*cacert)
        .emit(dh);
      return failure::promise();
    }
  }
  if (skip_peer_verification) {
    diagnostic::warning(
      "skipping peer verification allows man in the middle attacks")
      .hint("consider using a private CA")
      .emit(dh);
  }
  return {};
}

auto ssl_options::apply_to(
  curl::easy& easy, std::string_view cacert_fallback) const -> caf::error {
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
  check(
    easy.set(CURLOPT_USE_SSL, tls.inner ? CURLUSESSL_ALL : CURLUSESSL_NONE));
  check(easy.set(CURLOPT_SSL_VERIFYPEER, skip_peer_verification ? 0 : 1));
  check(easy.set(CURLOPT_SSL_VERIFYHOST, skip_peer_verification ? 0 : 1));
  return {};
}

auto ssl_options::apply_to(curl::easy& easy,
                           operator_control_plane& ctrl) const -> caf::error {
  if (not cacert.has_value()) {
    return apply_to(easy, query_cacert_fallback(ctrl));
  }
  return apply_to(easy);
}

auto ssl_options::query_cacert_fallback(operator_control_plane& ctrl)
  -> std::string {
  auto& config = ctrl.self().system().config();
  if (auto* v = caf::get_if<std::string>(&config.content, "tenzir.cacert")) {
    return std::move(*v);
  }
  return {};
}

auto ssl_options::update_cacert(operator_control_plane& ctrl) -> void {
  if (not cacert.has_value()) {
    cacert = located{query_cacert_fallback(ctrl), location::unknown};
  }
}

} // namespace tenzir
