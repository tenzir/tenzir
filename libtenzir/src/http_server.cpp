//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_server.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/concept/parseable/tenzir/endpoint.hpp"

#include <folly/ScopeGuard.h>
#include <folly/synchronization/Baton.h>
#include <proxygen/lib/http/coro/HTTPByteEvents.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/HTTPSourceFilter.h>
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

class ResponseCompletion final : public proxygen::coro::HTTPByteEventCallback {
public:
  ResponseCompletion(folly::Function<void()> kernel_write_callback,
                     folly::Function<void()> delivery_callback)
    : kernel_write_callback_{std::move(kernel_write_callback)},
      delivery_callback_{std::move(delivery_callback)} {
  }

  auto onByteEvent(proxygen::coro::HTTPByteEvent event) -> void override {
    switch (event.type) {
      case proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE:
        finish_kernel_write();
        if (ack_finished_) {
          finish();
        }
        break;
      case proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK:
        finish();
        break;
      case proxygen::coro::HTTPByteEvent::Type::TRANSPORT_WRITE:
      case proxygen::coro::HTTPByteEvent::Type::NIC_TX:
        break;
    }
  }

  auto onByteEventCanceled(proxygen::coro::HTTPByteEvent event,
                           proxygen::coro::HTTPError) -> void override {
    switch (event.type) {
      case proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE:
        finish_kernel_write();
        finish();
        break;
      case proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK:
        ack_finished_ = true;
        if (kernel_write_finished_) {
          finish();
        }
        break;
      case proxygen::coro::HTTPByteEvent::Type::TRANSPORT_WRITE:
      case proxygen::coro::HTTPByteEvent::Type::NIC_TX:
        break;
    }
  }

  auto finish() -> void {
    if (finished_) {
      return;
    }
    finish_kernel_write();
    finished_ = true;
    delivery_callback_();
    if (numWeakRefCountedPtrs() == 0) {
      delete this;
    }
  }

private:
  auto onWeakRefCountedPtrDestroy() -> void override {
    if (finished_ and numWeakRefCountedPtrs() == 0) {
      delete this;
    }
  }

  auto finish_kernel_write() -> void {
    if (kernel_write_finished_) {
      return;
    }
    kernel_write_finished_ = true;
    kernel_write_callback_();
  }

  folly::Function<void()> kernel_write_callback_;
  folly::Function<void()> delivery_callback_;
  bool kernel_write_finished_ = false;
  bool ack_finished_ = false;
  bool finished_ = false;
};

class DeliveryTrackingSource final : public proxygen::coro::HTTPSourceFilter {
public:
  DeliveryTrackingSource(proxygen::coro::HTTPSourceHolder source,
                         folly::Function<void()> kernel_write_callback,
                         folly::Function<void()> delivery_callback)
    : HTTPSourceFilter{source.release()},
      completion_{new ResponseCompletion{std::move(kernel_write_callback),
                                         std::move(delivery_callback)}} {
    setHeapAllocated();
  }

  ~DeliveryTrackingSource() override {
    finish_without_delivery_event();
  }

  auto readHeaderEvent()
    -> folly::coro::Task<proxygen::coro::HTTPHeaderEvent> override {
    auto event = co_await folly::coro::co_awaitTry(readHeaderEventImpl());
    auto guard = folly::makeGuard(lifetime(event));
    if (event.hasException()) {
      finish_without_delivery_event();
      co_yield folly::coro::co_error(proxygen::coro::getHTTPError(event));
    }
    if (event->eom) {
      track_delivery(event->byteEventRegistrations);
    }
    co_return std::move(*event);
  }

  auto readBodyEvent(uint32_t max)
    -> folly::coro::Task<proxygen::coro::HTTPBodyEvent> override {
    auto event = co_await folly::coro::co_awaitTry(readBodyEventImpl(max));
    auto guard = folly::makeGuard(lifetime(event));
    if (event.hasException()) {
      finish_without_delivery_event();
      co_yield folly::coro::co_error(proxygen::coro::getHTTPError(event));
    }
    if (event->eom) {
      track_delivery(event->byteEventRegistrations);
    }
    co_return std::move(*event);
  }

  auto stopReading(folly::Optional<const proxygen::coro::HTTPErrorCode> error
                   = folly::none) noexcept -> void override {
    finish_without_delivery_event();
    HTTPSourceFilter::stopReading(error);
  }

private:
  auto track_delivery(
    std::vector<proxygen::coro::HTTPByteEventRegistration>& registrations)
    -> void {
    TENZIR_ASSERT(completion_);
    auto registration = proxygen::coro::HTTPByteEventRegistration{};
    registration.events
      = static_cast<uint8_t>(proxygen::coro::HTTPByteEvent::Type::KERNEL_WRITE)
        | static_cast<uint8_t>(
          proxygen::coro::HTTPByteEvent::Type::CUMULATIVE_ACK);
    registration.callback = completion_->getWeakRefCountedPtr();
    registrations.emplace_back(std::move(registration));
    completion_ = nullptr;
  }

  auto finish_without_delivery_event() noexcept -> void {
    if (auto* completion = std::exchange(completion_, nullptr)) {
      completion->finish();
    }
  }

  ResponseCompletion* completion_;
};

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
                          tls.password ? tls.password->inner : "");
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
                    caf::actor_system_config const& /*cfg*/) -> bool {
  if (not tls) {
    return false;
  }
  // The TLS arg is explicitly set, so consulting node config would not
  // change the result; reading the explicit value is enough.
  auto tls_opts = tls_options::from_optional(tls, {.tls_default = false,
                                                   .is_server = true});
  return tls_opts.get_tls().inner;
}

auto make_config(std::string_view endpoint, location endpoint_location,
                 Option<located<data>> const& tls,
                 caf::actor_system_config const& cfg, diagnostic_handler& dh,
                 std::string_view argument_name)
  -> failure_or<proxygen::coro::HTTPServer::Config> {
  auto parsed = parse_endpoint(endpoint, endpoint_location, dh, argument_name);
  if (not parsed) {
    return failure::promise();
  }
  auto tls_enabled = is_tls_enabled(tls, cfg);
  if (parsed->scheme_tls) {
    if (*parsed->scheme_tls and not tls_enabled) {
      diagnostic::error("`https://` endpoint requires `tls=true`")
        .primary(endpoint_location)
        .emit(dh);
      return failure::promise();
    }
    if (not *parsed->scheme_tls and tls_enabled) {
      diagnostic::error("`http://` endpoint requires `tls=false`")
        .primary(endpoint_location)
        .emit(dh);
      return failure::promise();
    }
  }
  auto config = proxygen::coro::HTTPServer::Config{};
  try {
    config.socketConfig.bindAddress.setFromHostPort(parsed->host, parsed->port);
  } catch (std::exception const& ex) {
    diagnostic::error("failed to configure HTTP endpoint: {}", ex.what())
      .primary(endpoint_location)
      .emit(dh);
    return failure::promise();
  }
  config.numIOThreads = 1;
  if (tls_enabled) {
    auto options = tls_options::from_optional(tls, {.tls_default = false,
                                                    .is_server = true});
    auto resolved = options.resolve(cfg, dh);
    if (not resolved) {
      return failure::promise();
    }
    auto tls_config = make_ssl_context_config(*resolved, endpoint_location, dh);
    if (not tls_config) {
      return failure::promise();
    }
    config.socketConfig.sslContextConfigs.emplace_back(std::move(*tls_config));
  }
  config.shutdownOnSignals = {};
  return config;
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

auto track_response_delivery(proxygen::coro::HTTPSourceHolder response,
                             folly::Function<void()> callback)
  -> proxygen::coro::HTTPSourceHolder {
  return track_response_delivery(
    std::move(response), [] {}, std::move(callback));
}

auto track_response_delivery(proxygen::coro::HTTPSourceHolder response,
                             folly::Function<void()> kernel_write_callback,
                             folly::Function<void()> delivery_callback)
  -> proxygen::coro::HTTPSourceHolder {
  return proxygen::coro::HTTPSourceHolder{new DeliveryTrackingSource{
    std::move(response), std::move(kernel_write_callback),
    std::move(delivery_callback)}};
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

Server::Server(std::unique_ptr<ScopedServer> server) noexcept
  : server_{std::in_place,
            Box<ScopedServer>::from_non_null(std::move(server))} {
}

Server::~Server() {
  force_stop();
}

auto Server::start(proxygen::coro::HTTPServer::Config config,
                   std::shared_ptr<proxygen::coro::HTTPHandler> handler)
  -> Task<Result<Box<Server>, std::string>> {
  auto result = co_await spawn_blocking(
    [config = std::move(config), handler = std::move(handler)]() mutable {
      return ScopedServer::start(std::move(config), std::move(handler));
    });
  if (result.is_err()) {
    co_return Err{std::move(result).unwrap_err()};
  }
  co_return Box<Server>{std::in_place, std::move(result).unwrap()};
}

auto Server::drain() -> void {
  if (server_) {
    (*server_)->server().drain();
  }
}

auto Server::finish() -> void {
  if (server_) {
    std::thread{[server = std::exchange(server_, None{})] {}}.detach();
  }
}

auto Server::force_stop() -> void {
  if (server_) {
    (*server_)->server().forceStop();
    finish();
  }
}

} // namespace tenzir::http_server
