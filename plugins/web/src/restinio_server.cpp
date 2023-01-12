//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/restinio_server.hpp"

namespace vast::plugins::web {

server_t make_server(server_config config, std::unique_ptr<router_t> router,
                     restinio::io_context_holder_t io_context) {
  if (!config.require_tls) {
    return std::make_unique<dev_server_t>(io_context, [&](auto& settings) {
      settings.port(config.port)
        .address(config.bind_address)
        .request_handler(std::move(router));
    });
  } else {
    asio::ssl::context tls_context{asio::ssl::context::tls};
    // Most examples also set `asio::ssl::context::default_workarounds`, but
    // based on [1] these are only relevant for SSL which we don't support
    // anyways.
    // [1]: https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_set_options.html
    tls_context.set_options(asio::ssl::context::tls_server
                            | asio::ssl::context::single_dh_use);
    if (config.require_clientcerts)
      tls_context.set_verify_mode(
        asio::ssl::context::verify_peer
        | asio::ssl::context::verify_fail_if_no_peer_cert);
    else
      tls_context.set_verify_mode(asio::ssl::context::verify_none);
    tls_context.use_certificate_chain_file(config.certfile);
    tls_context.use_private_key_file(config.keyfile, asio::ssl::context::pem);
    // Manually specifying DH parameters is deprecated in favor of using
    // the OpenSSL built-in defaults, but asio has not been updated to
    // expose this API so we need to use the raw context.
    SSL_CTX_set_dh_auto(tls_context.native_handle(), true);
    using namespace std::literals::chrono_literals;
    return std::make_unique<tls_server_t>(io_context, [&](auto& settings) {
      settings.address(config.bind_address)
        .port(config.port)
        .request_handler(std::move(router))
        .read_next_http_message_timelimit(10s)
        .write_http_response_timelimit(1s)
        .handle_request_timeout(1s)
        .tls_context(std::move(tls_context));
    });
  }
}

} // namespace vast::plugins::web
