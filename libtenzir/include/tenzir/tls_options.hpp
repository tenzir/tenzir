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

  /// Applies the options to a `curl::easy` object.
  ///
  /// Precondition: if a node config is available, `apply_config()` has been
  /// called on this `tls_options` first so that the cached members reflect
  /// the effective values.
  auto apply_to(curl::easy& easy, std::string_view url) const -> caf::error;

  /// Precondition: `apply_config()` has been called on this `tls_options` if a
  /// node config is available.
  auto make_caf_context(operator_control_plane& ctrl,
                        std::optional<caf::uri> uri = std::nullopt) const
    -> caf::expected<caf::net::ssl::context>;

  /// Creates a folly SSL context from the TLS options.
  /// Returns nullptr if TLS is disabled and not required by the caller,
  /// a configured context on success, or failure on error
  /// (diagnostics emitted via dh).
  ///
  /// Precondition: `apply_config()` has been called on this `tls_options` if a
  /// node config is available.
  auto make_folly_ssl_context(diagnostic_handler& dh, bool tls_required
                                                      = false) const
    -> failure_or<std::shared_ptr<folly::SSLContext>>;

  /// Merges `tenzir.tls.*` (and legacy `tenzir.cacert`) node-config defaults
  /// into `*this`. After this call, every getter returns the effective value.
  /// Safe to call more than once; later calls do not overwrite values that
  /// were already set on the operator.
  ///
  /// Call this once where a config first becomes available -- typically at
  /// the top of the operator's `start()` or `operator()`. Parse-time
  /// validation, where no config exists yet, simply skips this call: the
  /// getters then return only operator-level values.
  auto apply_config(const caf::actor_system_config& cfg) -> void;
  auto apply_config(operator_control_plane& ctrl) -> void;

  /// Updates a URL using the `tls` option.
  ///
  /// Precondition: `apply_config()` has been called on this `tls_options` if a
  /// node config is available.
  [[nodiscard]] auto update_url(std::string_view url) const -> std::string;

  // -- getters ---------------------------------------------------------------
  //
  // The getters return the effective value, considering (in order):
  //   1. The `tls` record entry on the operator.
  //   2. The legacy top-level option (e.g. `cacert=...`), if any.
  //   3. Node-config defaults previously merged in via `apply_config()`.
  //
  // At parse/describe time, callers simply skip step 3 by not calling
  // `apply_config()`; the getter then sees only operator-level values, which
  // is exactly what parse-time validation needs.
  //
  // At runtime, call `apply_config()` once -- typically at the top of the
  // operator's `start()` or `operator()` -- so that all later getter calls
  // see node-config defaults.

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
