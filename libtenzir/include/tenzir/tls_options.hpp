//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser2.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/data.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/net/fwd.hpp>

#include <memory>
#include <optional>
#include <string_view>

namespace folly {
class SSLContext;
} // namespace folly

namespace tenzir {

struct operator_control_plane;

auto parse_curl_tls_version(std::string_view version) -> caf::expected<long>;
auto parse_openssl_tls_version(std::string_view version) -> caf::expected<int>;

auto parse_caf_tls_version(std::string_view version)
  -> caf::expected<caf::net::ssl::tls>;

auto add_tls_client_diagnostic_hints(diagnostic_builder diag, bool tls_enabled,
                                     std::string_view service_name = {},
                                     std::optional<uint64_t> plaintext_port
                                     = std::nullopt,
                                     std::optional<uint64_t> tls_port
                                     = std::nullopt) -> diagnostic_builder;

class tls_options;

/// Resolved, validated, ready-to-use TLS settings.
///
/// The only way to obtain a `TlsConfig` is via `tls_options::resolve()`, which
/// gates construction on both `validate()` passing and a node config being
/// available. As a result, holding a `TlsConfig` is a type-level guarantee
/// that all TLS settings are in their final, effective form -- there is no
/// "did you call apply_config?" question to ask.
///
/// All runtime TLS operations live as member functions here. The parse-time
/// API (input collection, validation, describer integration) stays on
/// `tls_options`.
struct TlsConfig {
  located<bool> tls;
  located<bool> skip_peer_verification;
  Option<located<std::string>> cacert;
  Option<located<std::string>> certfile;
  Option<located<std::string>> keyfile;
  Option<located<std::string>> password;
  Option<located<std::string>> tls_min_version;
  Option<located<std::string>> tls_ciphers;
  Option<located<std::string>> tls_client_ca;
  located<bool> tls_require_client_cert;

  /// Whether the originating `tls_options` was constructed with
  /// `options::uses_curl_http`. Controls scheme rewriting in `update_url`.
  bool uses_curl_http = false;

  /// Source location of the originating `tls=...` argument, if any. Used to
  /// clamp cipher-list diagnostic spans so a multi-line record literal does
  /// not highlight unrelated code. `location::unknown` if no `tls` argument
  /// was provided.
  location tls_arg_source = location::unknown;

  /// Applies the resolved options to a `curl::easy` object.
  auto apply_to(curl::easy& easy, std::string_view url) const -> caf::error;

  /// Updates a URL using the `tls` option (e.g. rewriting `http://` to
  /// `https://` when TLS is on and the option was explicit).
  [[nodiscard]] auto update_url(std::string_view url) const -> std::string;

  /// Creates a CAF SSL context from the resolved options.
  auto make_caf_context(operator_control_plane& ctrl,
                        std::optional<caf::uri> uri = std::nullopt) const
    -> caf::expected<caf::net::ssl::context>;

  /// Creates a folly SSL context from the resolved options.
  /// Returns nullptr if TLS is disabled and not required by the caller,
  /// a configured context on success, or failure on error
  /// (diagnostics emitted via dh).
  auto make_folly_ssl_context(diagnostic_handler& dh, bool tls_required
                                                      = false) const
    -> failure_or<std::shared_ptr<folly::SSLContext>>;

  /// Escape hatch for utility code that does not have access to an
  /// `actor_system_config` and just needs a `TlsConfig` with default settings
  /// (TLS enabled, no certificates, system-default verification). Operator
  /// code should always go through `tls_options::resolve`.
  static auto defaults() -> TlsConfig;

private:
  TlsConfig() = default;
  friend class tls_options;
};

class tls_options {
public:
  struct options {
    bool tls_default = true;
    bool uses_curl_http = false;
    bool is_server = false;
  };

  explicit tls_options(options opts)
    : uses_curl_http_{opts.uses_curl_http},
      is_server_{opts.is_server},
      tls_{located{data{opts.tls_default}, location::unknown}} {
  }
  explicit tls_options(located<data> tls_val);
  explicit tls_options(located<data> tls_val, options opts);
  tls_options() = default;

  static auto from_optional(Option<located<data>> const& tls,
                            options opts = {.tls_default = true,
                                            .uses_curl_http = false,
                                            .is_server = false})
    -> tls_options {
    return tls ? tls_options{*tls, opts} : tls_options{opts};
  }

  auto add_tls_options(argument_parser2&) -> void;

  /// Adds only the `tls` argument (deprecated individual options are omitted)
  /// to a new-executor `Describer`. Returns a `Validator` that reuses the
  /// existing `validate(diagnostic_handler&)` logic.
  template <class Args, class... Impls>
  auto add_to_describer(Describer<Args, Impls...>& d,
                        Option<located<data>> Args::* tls_member)
    -> _::operator_plugin::Validator {
    auto tls_arg = d.named("tls", tls_member, "record");
    auto opts
      = options{.uses_curl_http = uses_curl_http_, .is_server = is_server_};
    return [tls_arg, opts](DescribeCtx& ctx) -> Empty {
      auto tls_opt = from_optional(ctx.get(tls_arg), opts);
      (void)tls_opt.validate(ctx);
      return {};
    };
  }

  auto validate(diagnostic_handler&) const -> failure_or<void>;

  /// Ensures the internal consistency of the options, additionally
  /// considering the scheme in the URL.
  auto validate(const located<std::string>& url, diagnostic_handler&) const
    -> failure_or<void>;
  /// Ensures the internal consistency of the options, additionally
  /// considering the scheme in the URL.
  auto validate(std::string_view url, location url_loc,
                diagnostic_handler&) const -> failure_or<void>;

  /// Validates these options, merges in node-config defaults, and returns a
  /// resolved `TlsConfig` ready for runtime use. Diagnostics are emitted via
  /// `dh` on validation failure.
  ///
  /// This is the only path to obtain a `TlsConfig`. Runtime code should call
  /// `resolve()` exactly once -- typically at the top of the operator's
  /// `start()` or `operator()` -- and then use the returned `TlsConfig` for
  /// all subsequent TLS operations.
  auto resolve(const caf::actor_system_config& cfg,
               diagnostic_handler& dh) const -> failure_or<TlsConfig>;
  auto resolve(operator_control_plane& ctrl) const -> failure_or<TlsConfig>;

  /// Same as `resolve`, but additionally checks that the URL scheme is
  /// consistent with the TLS setting (e.g. `https://` requires `tls=true`).
  /// Use this overload when the URL is only available at runtime (e.g.
  /// because it contains secrets resolved at runtime).
  auto resolve(std::string_view url, location url_loc,
               const caf::actor_system_config& cfg,
               diagnostic_handler& dh) const -> failure_or<TlsConfig>;

  // -- getters ---------------------------------------------------------------
  //
  // These getters return what is currently set on the operator (the `tls`
  // record entry, or the legacy top-level options). They do NOT consult the
  // node config -- that is `resolve()`'s job. The getters are intended for
  // parse-time validation (inside `validate()` and describer validators);
  // runtime code should obtain a `TlsConfig` via `resolve()` and read from
  // its fields instead.

  auto get_tls() const -> located<bool>;
  auto get_skip_peer_verification() const -> located<bool>;
  auto get_cacert() const -> std::optional<located<std::string>>;
  auto get_certfile() const -> std::optional<located<std::string>>;
  auto get_keyfile() const -> std::optional<located<std::string>>;
  auto get_password() const -> std::optional<located<std::string>>;
  auto get_tls_min_version() const -> std::optional<located<std::string>>;
  auto get_tls_ciphers() const -> std::optional<located<std::string>>;
  auto get_tls_client_ca() const -> std::optional<located<std::string>>;
  auto get_tls_require_client_cert() const -> located<bool>;

private:
  auto get_record_bool(std::string_view key) const
    -> std::optional<located<bool>>;
  auto get_record_string(std::string_view key) const
    -> std::optional<located<std::string>>;
  auto validate_tls_record(diagnostic_handler& dh) const -> failure_or<void>;

  // Merges `tenzir.tls.*` (and legacy `tenzir.cacert`) node-config defaults
  // into the cached members. Used internally by `resolve()`; not exposed
  // publicly because the only safe way to consume resolved settings is via
  // a `TlsConfig`.
  auto apply_config(const caf::actor_system_config& cfg) -> void;

  bool uses_curl_http_ = false;
  bool is_server_ = false;
  mutable std::optional<located<data>> tls_;
  mutable std::optional<located<bool>> skip_peer_verification_;
  mutable std::optional<located<std::string>> cacert_;
  mutable std::optional<located<std::string>> certfile_;
  mutable std::optional<located<std::string>> keyfile_;
  mutable std::optional<located<std::string>> password_;
  mutable std::optional<located<std::string>> tls_min_version_;
  mutable std::optional<located<std::string>> tls_ciphers_;
  mutable std::optional<located<std::string>> tls_client_ca_;
  mutable std::optional<located<bool>> tls_require_client_cert_;

  friend auto inspect(auto& f, tls_options& x) -> bool {
    return f.object(x).fields(
      f.field("uses_curl_http", x.uses_curl_http_),
      f.field("is_server", x.is_server_), f.field("tls", x.tls_),
      f.field("skip_peer_verification", x.skip_peer_verification_),
      f.field("cacert", x.cacert_), f.field("certfile", x.certfile_),
      f.field("keyfile", x.keyfile_), f.field("password", x.password_),
      f.field("tls_min_version", x.tls_min_version_),
      f.field("tls_ciphers", x.tls_ciphers_),
      f.field("tls_client_ca", x.tls_client_ca_),
      f.field("tls_require_client_cert", x.tls_require_client_cert_));
  }
};

} // namespace tenzir
