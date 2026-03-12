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

  auto add_tls_options(argument_parser2&) -> void;

  /// Adds only the `tls` argument (deprecated individual options are omitted)
  /// to a new-executor `Describer`. Returns a `Validator` that reuses the
  /// existing `validate(diagnostic_handler&)` logic.
  template <class Args, class... Impls>
  auto add_to_describer(Describer<Args, Impls...>& d,
                        std::optional<located<data>> Args::* tls_member)
    -> _::operator_plugin::Validator {
    auto tls_arg = d.named("tls", tls_member, "record");
    auto opts
      = options{.uses_curl_http = uses_curl_http_, .is_server = is_server_};
    return [tls_arg, opts](DescribeCtx& ctx) -> Empty {
      auto tls_val = ctx.get(tls_arg);
      auto tls_opt = tls_val ? tls_options{*tls_val, opts} : tls_options{opts};
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

  /// Applies the options to a `curl::easy` object, potentially getting
  /// `tenzir.cacert` as a `cacert_fallbacl` if none is set explicitly.
  auto apply_to(curl::easy& easy, std::string_view url,
                operator_control_plane* ctrl) const -> caf::error;

  auto make_caf_context(operator_control_plane& ctrl,
                        std::optional<caf::uri> uri = std::nullopt) const
    -> caf::expected<caf::net::ssl::context>;

  /// Creates a folly SSL context from the TLS options.
  /// Returns nullptr if TLS is disabled, a configured context on success,
  /// or failure on error (diagnostics emitted via dh).
  auto make_folly_ssl_context(diagnostic_handler& dh) const
    -> failure_or<std::shared_ptr<folly::SSLContext>>;

  /// Updates values in *this using the config.
  auto update_from_config(operator_control_plane& ctrl) -> void;
  auto update_from_config(const caf::actor_system_config* cfg) -> void;

  /// Updates a URL using the `tls` option
  [[nodiscard]] auto
  update_url(std::string_view url, operator_control_plane* ctrl) const
    -> std::string;

  /// Get the value of the TLS option, or the config setting
  auto get_tls(operator_control_plane* ctrl) const -> located<bool>;
  auto get_tls(const caf::actor_system_config* cfg) const -> located<bool>;
  auto get_tls(std::nullptr_t) const -> located<bool> {
    return get_tls(static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_skip_peer_verification(operator_control_plane* ctrl) const
    -> located<bool>;
  auto get_skip_peer_verification(const caf::actor_system_config* cfg) const
    -> located<bool>;
  auto get_skip_peer_verification(std::nullptr_t) const -> located<bool> {
    return get_skip_peer_verification(
      static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_cacert(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_cacert(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_cacert(std::nullptr_t) const -> std::optional<located<std::string>> {
    return get_cacert(static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_certfile(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_certfile(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_certfile(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_certfile(static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_keyfile(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_keyfile(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_keyfile(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_keyfile(static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_password(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_password(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_password(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_password(static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_tls_min_version(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_tls_min_version(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_tls_min_version(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_tls_min_version(
      static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_tls_ciphers(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_tls_ciphers(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_tls_ciphers(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_tls_ciphers(
      static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_tls_client_ca(operator_control_plane* ctrl) const
    -> std::optional<located<std::string>>;
  auto get_tls_client_ca(const caf::actor_system_config* cfg) const
    -> std::optional<located<std::string>>;
  auto get_tls_client_ca(std::nullptr_t) const
    -> std::optional<located<std::string>> {
    return get_tls_client_ca(
      static_cast<const caf::actor_system_config*>(nullptr));
  }

  auto get_tls_require_client_cert(operator_control_plane* ctrl) const
    -> located<bool>;
  auto get_tls_require_client_cert(const caf::actor_system_config* cfg) const
    -> located<bool>;
  auto get_tls_require_client_cert(std::nullptr_t) const -> located<bool> {
    return get_tls_require_client_cert(
      static_cast<const caf::actor_system_config*>(nullptr));
  }

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
