//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tls_options.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_system_config.hpp>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/SSLContext.h>
#include <folly/io/async/ssl/SSLErrors.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <tuple>

using namespace tenzir;

namespace {

auto expected_peer_name(folly::SSLContext const& ctx) -> std::string {
  auto* param = SSL_CTX_get0_param(ctx.getSSLCtx());
  auto const* host = X509_VERIFY_PARAM_get0_host(param, 0);
  return host ? std::string{host} : std::string{};
}

/// A self-signed certificate and its key.
struct SelfSigned {
  std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key{nullptr,
                                                          EVP_PKEY_free};
  std::unique_ptr<X509, decltype(&X509_free)> cert{nullptr, X509_free};
};

/// Creates a self-signed certificate for the common name `name`.
auto make_self_signed(char const* name) -> SelfSigned {
  auto result = SelfSigned{};
  result.key.reset(EVP_RSA_gen(2048));
  REQUIRE(result.key);
  result.cert.reset(X509_new());
  auto* cert = result.cert.get();
  X509_set_version(cert, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), 0);
  X509_gmtime_adj(X509_getm_notAfter(cert), 3600);
  X509_set_pubkey(cert, result.key.get());
  auto* subject = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                             reinterpret_cast<unsigned char const*>(name), -1,
                             -1, 0);
  X509_set_issuer_name(cert, subject);
  REQUIRE(X509_sign(cert, result.key.get(), EVP_sha256()) > 0);
  return result;
}

/// Runs a TLS handshake in memory between a client using `client` and a
/// server presenting `server`. Returns the first error of the thread's error
/// queue after a failed client handshake, which is what folly and Proxygen
/// put into the exception, or 0 on success. A successful handshake leaves the
/// error queue as it is.
auto handshake(folly::SSLContext const& client, SelfSigned const& server,
               bool verify = true) -> unsigned long {
  ERR_clear_error();
  auto* server_ctx = SSL_CTX_new(TLS_server_method());
  REQUIRE(SSL_CTX_use_certificate(server_ctx, server.cert.get()) == 1);
  REQUIRE(SSL_CTX_use_PrivateKey(server_ctx, server.key.get()) == 1);
  auto* client_ssl = SSL_new(client.getSSLCtx());
  auto* server_ssl = SSL_new(server_ctx);
  // folly applies the verification mode to each connection in the same way.
  SSL_set_verify(client_ssl, verify ? SSL_VERIFY_PEER : SSL_VERIFY_NONE,
                 nullptr);
  BIO* client_bio = nullptr;
  BIO* server_bio = nullptr;
  REQUIRE(BIO_new_bio_pair(&client_bio, 0, &server_bio, 0) == 1);
  SSL_set_bio(client_ssl, client_bio, client_bio);
  SSL_set_bio(server_ssl, server_bio, server_bio);
  SSL_set_connect_state(client_ssl);
  SSL_set_accept_state(server_ssl);
  auto error = 0ul;
  // Alternate until the handshake completes or the client gives up.
  for (auto i = 0; i < 100; ++i) {
    auto const client_result = SSL_do_handshake(client_ssl);
    if (client_result <= 0
        and SSL_get_error(client_ssl, client_result) == SSL_ERROR_SSL) {
      error = ERR_get_error();
      ERR_clear_error();
      break;
    }
    auto const server_result = SSL_do_handshake(server_ssl);
    if (client_result == 1 and server_result == 1) {
      break;
    }
  }
  SSL_free(client_ssl);
  SSL_free(server_ssl);
  SSL_CTX_free(server_ctx);
  return error;
}

/// Wraps an OpenSSL error into the exception of a failed folly handshake.
auto handshake_exception(unsigned long error) -> folly::exception_wrapper {
  return folly::make_exception_wrapper<folly::SSLException>(SSL_ERROR_SSL,
                                                            error, -1, 0);
}

/// Writes `cert` to a temporary PEM file.
auto write_pem(X509* cert) -> std::filesystem::path {
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tenzir-tls-options-{}.pem", uuid::random());
  auto* file = BIO_new_file(path.c_str(), "w");
  REQUIRE(file);
  REQUIRE(PEM_write_bio_X509(file, cert) == 1);
  BIO_free(file);
  return path;
}

} // namespace

TEST("TLS enablement can come from node config") {
  auto cfg = caf::actor_system_config{};
  caf::put(cfg.content, "tenzir.tls.enable", true);
  auto opts = tls_options{{.tls_default = false, .is_server = true}};
  auto dh = collecting_diagnostic_handler{};
  auto resolved = opts.resolve(cfg, dh);
  REQUIRE(resolved);
  REQUIRE(dh.empty());
  CHECK(resolved->tls.inner);
}

TEST("mTLS client CA can come from node config") {
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tenzir-tls-options-{}.pem", uuid::random());
  auto file = std::ofstream{path};
  file << "test CA";
  file.close();
  auto cfg = caf::actor_system_config{};
  caf::put(cfg.content, "tenzir.tls.tls-client-ca", path.string());
  auto opts = tls_options{
    located{data{record{{"require_client_cert", true}}}, location::unknown},
    {.is_server = true},
  };
  auto dh = collecting_diagnostic_handler{};
  auto resolved = opts.resolve(cfg, dh);
  std::ignore = std::filesystem::remove(path);
  REQUIRE(resolved);
  REQUIRE(dh.empty());
  REQUIRE(resolved->tls_client_ca);
  CHECK_EQUAL(resolved->tls_client_ca->inner, path.string());
}

TEST("HTTP client TLS verifies each host against its own name") {
  auto dh = collecting_diagnostic_handler{};
  auto tls = HttpClientTls::make(TlsConfig::defaults(), "api.example", dh);
  REQUIRE(tls);
  REQUIRE(dh.empty());
  auto api = tls->context_for("api.example");
  REQUIRE(api.is_ok());
  auto api_ctx = std::move(api).unwrap();
  REQUIRE(api_ctx);
  CHECK_EQUAL(expected_peer_name(*api_ctx), "api.example");
  // Pools for the prepared host share its context.
  auto again = tls->context_for("api.example");
  REQUIRE(again.is_ok());
  CHECK_EQUAL(std::move(again).unwrap().get(), api_ctx.get());
  // Other hosts get their own context and leave the prepared one unchanged.
  auto token = tls->without_client_identity().context_for("login.example");
  REQUIRE(token.is_ok());
  auto token_ctx = std::move(token).unwrap();
  REQUIRE(token_ctx);
  CHECK_NOT_EQUAL(token_ctx.get(), api_ctx.get());
  CHECK_EQUAL(expected_peer_name(*token_ctx), "login.example");
  CHECK_EQUAL(expected_peer_name(*api_ctx), "api.example");
}

TEST("HTTP client TLS defaults to the system context") {
  auto result = HttpClientTls{}.context_for("api.example");
  REQUIRE(result.is_ok());
  CHECK(not std::move(result).unwrap());
}

TEST("TLS config without client identity keeps trust settings") {
  auto config = TlsConfig::defaults();
  config.cacert = located{std::string{"ca.pem"}, location::unknown};
  config.certfile = located{std::string{"client.pem"}, location::unknown};
  config.keyfile = located{std::string{"client.key"}, location::unknown};
  config.password = located{std::string{"secret"}, location::unknown};
  auto result = config.without_client_identity();
  CHECK(not result.certfile);
  CHECK(not result.keyfile);
  CHECK(not result.password);
  REQUIRE(result.cacert);
  CHECK_EQUAL(result.cacert->inner, "ca.pem");
}

TEST("a failed handshake carries its own verification failure") {
  auto trusted = make_self_signed("api.example");
  auto untrusted = make_self_signed("api.example");
  auto const cacert = write_pem(trusted.cert.get());
  auto config = TlsConfig::defaults();
  config.cacert = located{cacert.string(), location::unknown};
  auto dh = collecting_diagnostic_handler{};
  auto ctx = config.make_folly_ssl_context(dh, "api.example");
  std::ignore = std::filesystem::remove(cacert);
  REQUIRE(ctx);
  REQUIRE(*ctx);
  auto const failed = handshake(**ctx, untrusted);
  REQUIRE(failed != 0);
  // A concurrent connection of the same context succeeds before the failed
  // one reports its error, which must not change that error.
  CHECK_EQUAL(handshake(**ctx, trusted), 0ul);
  auto const failure = peer_verification_failure(failed);
  REQUIRE(failure);
  CHECK(failure->peer == TlsPeer::server);
  CHECK_EQUAL(failure->reason, "self-signed certificate");
  auto const description = describe_tls_verification_error(
    handshake_exception(failed), "api.example");
  REQUIRE(description);
  CHECK_EQUAL(*description, "TLS certificate verification failed for "
                            "`api.example`: self-signed certificate");
  // Proxygen rethrows failed connections as plain socket exceptions that keep
  // the OpenSSL error only in their message.
  auto const sliced
    = folly::make_exception_wrapper<folly::AsyncSocketException>(
      folly::AsyncSocketException::SSL_ERROR,
      folly::SSLException{SSL_ERROR_SSL, failed, -1, 0}.what());
  CHECK_EQUAL(describe_tls_verification_error(sliced, "api.example"),
              description);
  // Every failed connection describes its own failure.
  CHECK_EQUAL(describe_tls_verification_error(handshake_exception(failed),
                                              "api.example"),
              description);
}

TEST("a failed proxy handshake names the proxy") {
  auto untrusted = make_self_signed("proxy.example");
  auto dh = collecting_diagnostic_handler{};
  auto ctx = TlsConfig::defaults().make_folly_ssl_context(dh, "proxy.example");
  REQUIRE(ctx);
  REQUIRE(*ctx);
  record_peer_verification(**ctx, TlsPeer::proxy);
  auto const error = handshake_exception(handshake(**ctx, untrusted));
  CHECK_EQUAL(describe_tls_verification_error(
                error, "api.example", std::string_view{"proxy.example"}),
              "TLS certificate verification failed for proxy "
              "`proxy.example`: self-signed certificate");
  CHECK_EQUAL(describe_tls_verification_error(error, "api.example"),
              "TLS certificate verification failed for the proxy: "
              "self-signed certificate");
}

TEST("handshakes that skip verification report nothing") {
  auto untrusted = make_self_signed("api.example");
  auto dh = collecting_diagnostic_handler{};
  auto ctx = TlsConfig::defaults().make_folly_ssl_context(dh, "api.example");
  REQUIRE(ctx);
  REQUIRE(*ctx);
  CHECK_EQUAL(handshake(**ctx, untrusted, false), 0ul);
  CHECK_EQUAL(ERR_peek_error(), 0ul);
}

TEST("verification failures have readable OpenSSL error strings") {
  auto untrusted = make_self_signed("api.example");
  auto dh = collecting_diagnostic_handler{};
  auto ctx = TlsConfig::defaults().make_folly_ssl_context(dh, "api.example");
  REQUIRE(ctx);
  REQUIRE(*ctx);
  auto const failed = handshake(**ctx, untrusted);
  auto buffer = std::array<char, 256>{};
  ERR_error_string_n(failed, buffer.data(), buffer.size());
  auto const message = std::string{buffer.data()};
  CHECK(message.find("certificate verification") != std::string::npos);
  CHECK(message.find("self-signed certificate") != std::string::npos);
}

TEST("other failures are no verification failures") {
  using folly::AsyncSocketException;
  CHECK(not describe_tls_verification_error(
    folly::make_exception_wrapper<AsyncSocketException>(
      AsyncSocketException::NOT_OPEN, "connection refused"),
    "api.example"));
  CHECK(not describe_tls_verification_error(
    folly::make_exception_wrapper<std::runtime_error>("timeout"),
    "api.example"));
  // OpenSSL's own errors are not ours to decode.
  CHECK(not peer_verification_failure(ERR_PACK(ERR_LIB_SSL, 0, 134)));
  CHECK(not peer_verification_failure(0));
  CHECK_EQUAL(describe_tls_verification_error(
                folly::make_exception_wrapper<TlsVerificationError>("named"),
                "api.example"),
              "named");
}
