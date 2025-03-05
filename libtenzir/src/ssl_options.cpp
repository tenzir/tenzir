//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/ssl_options.hpp"

#include <filesystem>

namespace tenzir {

ssl_options::ssl_options(std::string_view default_cacert) {
  cacert = located{default_cacert, location::unknown};
}

auto ssl_options::add_url_option(argument_parser2& parser,
                                 bool positional) -> void {
  if (positional) {
    parser.positional("url", url);
  } else {
    parser.named("url", url);
  }
}

auto ssl_options::add_tls_options(argument_parser2& parser) -> void {
  parser.named("tls", tls)
    .named("skip_peer_verification", skip_peer_verification)
    .named("cacert", cacert)
    .named("certfile", certfile)
    .named("keyfile", keyfile);
}

auto ssl_options::validate(diagnostic_handler& dh) -> failure_or<void> {
  auto url_says_http = url.inner.starts_with("http://");
  auto url_says_https = url.inner.starts_with("https://");
  auto option_says_no_tls = tls && not tls->inner;
  if (url_says_https and option_says_no_tls) {
    diagnostic::error("conflicting TLS settings")
      .primary(url, "url specifies TLS")
      .primary(*tls, "option specifies no TLS")
      .emit(dh);
    return failure::promise();
  }
  if (not tls) {
    if (url_says_http) {
      tls = located{false, url.source};
    } else {
      tls = located{true, url.source};
    }
  }
  const auto tls_logic
    = [&](auto& thing, std::string_view name) -> failure_or<void> {
    if (tls->inner and thing) {
      diagnostic::error("`{}` requires TLS", name)
        .primary(tls->source, "TLS is disabled")
        .primary(*thing)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(tls_logic(skip_peer_verification, "skip_peer_verification"));
  TRY(tls_logic(cacert, "cacert"));
  TRY(tls_logic(certfile, "certfile"));
  TRY(tls_logic(keyfile, "keyfile"));
  if (tls->inner and not skip_peer_verification) {
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

auto ssl_options::apply_to(curl::easy& easy, diagnostic_handler& dh) -> bool {
  check(easy.set(CURLOPT_URL, url.inner));
  if (cacert) {
    if (auto ec = easy.set(CURLOPT_CAINFO, cacert->inner);
        ec != curl::easy::code::ok) {
      diagnostic::error("failed to set `cacert`: {}", to_string(ec)).emit(dh);
      return false;
    }
  }
  if (certfile) {
    if (auto ec = easy.set(CURLOPT_SSLCERT, certfile->inner);
        ec != curl::easy::code::ok) {
      diagnostic::error("failed to set `certfile`: {}", to_string(ec)).emit(dh);
      return false;
    }
  }
  if (keyfile) {
    if (auto ec = easy.set(CURLOPT_SSLKEY, keyfile->inner);
        ec != curl::easy::code::ok) {
      diagnostic::error("failed to set `keyfile`: {}", to_string(ec)).emit(dh);
      return false;
    }
  }
  TENZIR_ASSERT(tls.has_value());
  check(
    easy.set(CURLOPT_USE_SSL, tls->inner ? CURLUSESSL_ALL : CURLUSESSL_NONE));
  check(easy.set(CURLOPT_SSL_VERIFYPEER, skip_peer_verification ? 0 : 1));
  check(easy.set(CURLOPT_SSL_VERIFYHOST, skip_peer_verification ? 0 : 1));
  return true;
}
} // namespace tenzir
