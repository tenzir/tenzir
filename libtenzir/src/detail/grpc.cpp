//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/grpc.hpp"

#include "tenzir/detail/string.hpp"
#include "tenzir/try.hpp"

#include <fmt/format.h>

#include <fstream>

namespace tenzir::detail {

auto grpc_peer_ip(std::string_view peer) -> Result<std::string, std::string> {
  auto const address = peer;
  if (peer.starts_with("ipv4:")) {
    peer.remove_prefix(std::string_view{"ipv4:"}.size());
    auto const port = peer.rfind(':');
    if (port != std::string_view::npos) {
      return std::string{peer.substr(0, port)};
    }
  }
  if (peer.starts_with("ipv6:")) {
    peer.remove_prefix(std::string_view{"ipv6:"}.size());
    // gRPC percent-encodes the brackets delimiting the address.
    auto const decoded = percent_unescape(peer);
    auto decoded_view = std::string_view{decoded};
    if (decoded_view.starts_with('[')) {
      decoded_view.remove_prefix(1);
      if (auto const end = decoded_view.find(']');
          end != std::string_view::npos) {
        return std::string{decoded_view.substr(0, end)};
      }
    }
  }
  return Err{fmt::format("unsupported gRPC peer address `{}`", address)};
}

auto make_grpc_server_credentials(TlsConfig const& tls,
                                  location fallback_location)
  -> Result<std::shared_ptr<grpc::ServerCredentials>, diagnostic> {
  if (not tls.tls.inner) {
    return grpc::InsecureServerCredentials();
  }
  if (tls.tls_min_version) {
    return Err{
      diagnostic::error("`tls.min_version` is not supported for gRPC servers")
        .primary(*tls.tls_min_version)
        .done()};
  }
  if (tls.tls_ciphers) {
    return Err{
      diagnostic::error("`tls.ciphers` is not supported for gRPC servers")
        .primary(*tls.tls_ciphers)
        .note("gRPC exposes cipher selection only as a process-global setting")
        .done()};
  }
  if (tls.password) {
    return Err{diagnostic::error("password-protected private keys are not "
                                 "supported for gRPC servers")
                 .primary(*tls.password)
                 .done()};
  }
  if (not tls.certfile) {
    return Err{
      diagnostic::error("`tls.certfile` is required when TLS is enabled")
        .primary(fallback_location)
        .done()};
  }
  auto read_pem = [](located<std::string> const& path,
                     std::string_view name) -> Result<std::string, diagnostic> {
    auto input = std::ifstream{path.inner, std::ios::binary};
    if (not input) {
      return Err{
        diagnostic::error("failed to read `{}`", name).primary(path).done()};
    }
    return std::string{std::istreambuf_iterator<char>{input},
                       std::istreambuf_iterator<char>{}};
  };
  TRY(auto cert, read_pem(*tls.certfile, "tls.certfile"));
  auto const& key_path = tls.keyfile ? *tls.keyfile : *tls.certfile;
  TRY(auto key,
      read_pem(key_path, tls.keyfile ? "tls.keyfile" : "tls.certfile"));
  auto ssl = grpc::SslServerCredentialsOptions{};
  ssl.pem_key_cert_pairs.emplace_back(
    grpc::SslServerCredentialsOptions::PemKeyCertPair{
      .private_key = std::move(key), .cert_chain = std::move(cert)});
  auto append_roots = [&](Option<located<std::string>> const& client_ca,
                          std::string_view name) -> Result<void, diagnostic> {
    if (not client_ca) {
      return {};
    }
    TRY(auto roots, read_pem(*client_ca, name));
    ssl.pem_root_certs += std::move(roots);
    return {};
  };
  TRY(append_roots(tls.tls_client_ca, "tls.client_ca"));
  TRY(append_roots(tls.cacert, "tls.cacert"));
  auto const has_client_ca = tls.tls_client_ca or tls.cacert;
  if (tls.tls_require_client_cert.inner) {
    ssl.client_certificate_request
      = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
  } else if (has_client_ca and not tls.skip_peer_verification.inner) {
    ssl.client_certificate_request
      = GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;
  } else {
    ssl.client_certificate_request = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
  }
  return grpc::SslServerCredentials(ssl);
}

} // namespace tenzir::detail
