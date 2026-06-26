//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_server.hpp"

#include "tenzir/concept/parseable/tenzir/endpoint.hpp"

#include <folly/synchronization/Baton.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <proxygen/lib/utils/URL.h>

#include <exception>
#include <utility>

namespace tenzir::http_server {

namespace {

auto parse_server_folly_tls_version(std::string_view input)
  -> Option<folly::SSLContext::SSLVersion> {
  if (input == "" or input == "any" or input == "1.0") {
    return folly::SSLContext::SSLVersion::TLSv1;
  }
  if (input == "1.2") {
    return folly::SSLContext::SSLVersion::TLSv1_2;
  }
  if (input == "1.3") {
    return folly::SSLContext::SSLVersion::TLSv1_3;
  }
  return None{};
}

} // namespace

auto make_ssl_context_config(TlsConfig const& tls, location primary,
                             diagnostic_handler& dh)
  -> failure_or<wangle::SSLContextConfig> {
  if (not tls.certfile) {
    diagnostic::error("`tls.certfile` is required when TLS is enabled")
      .primary(primary)
      .emit(dh);
    return failure::promise();
  }
  auto& certfile = *tls.certfile;
  // wangle's `SSLContextConfig` only accepts a password *file path*, not an
  // inline secret, and proxygen offers no hook to install a custom password
  // collector. Rather than silently misinterpreting the password as a filename,
  // reject it with a clear diagnostic.
  if (auto& password = tls.password) {
    diagnostic::error(
      "inline `tls.password` is not supported for this operator")
      .primary(*password)
      .note("the underlying HTTP server can only read the key password from a "
            "file")
      .hint("use a private key in `tls.keyfile` that is not password-protected")
      .emit(dh);
    return failure::promise();
  }
  auto config = proxygen::coro::HTTPServer::getDefaultTLSConfig();
  if (auto& min = tls.tls_min_version) {
    if (not min->inner.empty()) {
      if (auto parsed = parse_server_folly_tls_version(min->inner)) {
        config.sslVersion = *parsed;
      } else {
        diagnostic::error("invalid TLS minimum version: `{}`", min->inner)
          .primary(*min)
          .hint("supported values are `1.0`, `1.2`, and `1.3`")
          .emit(dh);
        return failure::promise();
      }
    }
  }
  try {
    config.setCertificate(certfile.inner,
                          tls.keyfile ? tls.keyfile->inner : certfile.inner,
                          /*passwordPath=*/"");
  } catch (std::exception const& ex) {
    diagnostic::error("failed to load TLS certificate: {}", ex.what())
      .primary(certfile)
      .emit(dh);
    return failure::promise();
  }
  if (tls.tls_require_client_cert.inner) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::ALWAYS;
  } else if (tls.skip_peer_verification.inner) {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::DO_NOT_REQUEST;
  } else {
    config.clientVerification
      = folly::SSLContext::VerifyClientCertificate::IF_PRESENTED;
  }
  if (auto& ciphers = tls.tls_ciphers) {
    config.sslCiphers = ciphers->inner;
  }
  if (auto& client_ca = tls.tls_client_ca) {
    config.clientCAFiles.push_back(client_ca->inner);
  }
  if (auto& cacert = tls.cacert) {
    config.clientCAFiles.push_back(cacert->inner);
  }
  return config;
}

auto parse_endpoint(std::string_view endpoint, location loc,
                    diagnostic_handler& dh, std::string_view argument_name)
  -> Option<server_endpoint> {
  if (endpoint.contains("://")) {
    auto parsed = proxygen::URL{std::string{endpoint}};
    if (not parsed.isValid() or not parsed.hasHost()) {
      diagnostic::error("failed to parse endpoint URL").primary(loc).emit(dh);
      return None{};
    }
    auto scheme = parsed.getScheme();
    auto scheme_tls = Option<bool>{None{}};
    if (scheme == "https") {
      scheme_tls = true;
    } else if (scheme == "http") {
      scheme_tls = false;
    } else {
      diagnostic::error("unsupported endpoint URL scheme: `{}`", scheme)
        .primary(loc)
        .hint("use `http://` or `https://`")
        .emit(dh);
      return None{};
    }
    return server_endpoint{
      .host = parsed.getHost(),
      .port = parsed.getPort(),
      .scheme_tls = scheme_tls,
    };
  }
  if (endpoint.empty()) {
    diagnostic::error("`{}` must not be empty", argument_name)
      .primary(loc)
      .emit(dh);
    return None{};
  }
  auto parsed = tenzir::Endpoint{};
  if (not parsers::endpoint(endpoint, parsed)) {
    diagnostic::error("failed to parse endpoint")
      .primary(loc)
      .hint("expected `host:port`, `[host]:port`, or URL")
      .emit(dh);
    return None{};
  }
  if (not parsed.port) {
    diagnostic::error("endpoint port is missing").primary(loc).emit(dh);
    return None{};
  }
  if (parsed.port->type() != port_type::unknown
      and parsed.port->type() != port_type::tcp) {
    diagnostic::error("expected a TCP endpoint").primary(loc).emit(dh);
    return None{};
  }
  return server_endpoint{
    .host = std::move(parsed.host),
    .port = parsed.port->number(),
    .scheme_tls = None{},
  };
}

auto is_tls_enabled(Option<located<data>> const& tls,
                    const caf::actor_system_config& /*cfg*/) -> bool {
  if (not tls) {
    return false;
  }
  // The TLS arg is explicitly set, so consulting node config would not
  // change the result; reading the explicit value is enough.
  auto tls_opts = tls_options::from_optional(tls, {.tls_default = false,
                                                   .is_server = true});
  return tls_opts.get_tls().inner;
}

auto make_response(uint16_t status, const std::string& content_type,
                   std::string body) -> proxygen::coro::HTTPSourceHolder {
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedResponse(
    status, std::move(body));
  if (not content_type.empty()) {
    source->msg_->getHeaders().set(proxygen::HTTP_HEADER_CONTENT_TYPE,
                                   content_type);
  }
  return proxygen::coro::HTTPSourceHolder{source};
}

ScopedServer::ScopedServer(proxygen::coro::HTTPServer::Config config,
                           std::shared_ptr<proxygen::coro::HTTPHandler> handler)
  : server_{std::move(config), std::move(handler)} {
}

auto ScopedServer::start(proxygen::coro::HTTPServer::Config config,
                         std::shared_ptr<proxygen::coro::HTTPHandler> handler)
  -> Result<std::unique_ptr<ScopedServer>, std::string> {
  auto s = std::unique_ptr<ScopedServer>{
    new ScopedServer{std::move(config), std::move(handler)}};
  try {
    s->start_impl();
  } catch (std::exception const& ex) {
    return Err{std::string{ex.what()}};
  }
  return s;
}

void ScopedServer::start_impl() {
  std::exception_ptr eptr;
  folly::Baton baton;
  thread_ = std::thread{[&] {
    server_.start(
      [&] {
        baton.post();
      },
      [&](std::exception_ptr ex) {
        eptr = ex;
        baton.post();
      });
  }};
  baton.wait();
  if (eptr) {
    // The IO thread returned without starting the server; join it here. The
    // destructor checks `joinable()` so it will not try to join again while
    // unwinding the exception we are about to rethrow.
    thread_.join();
    std::rethrow_exception(eptr);
  }
}

ScopedServer::~ScopedServer() {
  if (not thread_.joinable()) {
    // `start_impl()` failed and already joined the IO thread. Calling
    // `drain()`/`forceStop()` on a server that never reached RUNNING is
    // unnecessary, and `thread_.join()` would throw `std::system_error`.
    return;
  }
  server_.drain();
  server_.forceStop();
  thread_.join();
}

} // namespace tenzir::http_server
