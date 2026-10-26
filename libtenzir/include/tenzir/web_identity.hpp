//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/secret.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant.hpp"

#include <caf/expected.hpp>

#include <string>
#include <utility>
#include <vector>

namespace tenzir {

/// Assigns the non-empty `string`, `blob`, or `secret` at `key` to `x`, if the
/// record has one.
auto assign_secret(const located<nova::Record>& config, std::string_view key,
                   Option<nova::Secret>& x, diagnostic_handler& dh)
  -> failure_or<void>;

/// Reveals an optional secret as UTF-8 text, or returns an empty string.
auto reveal_string(const Option<nova::Secret>& secret, std::string_view name,
                   location source, diagnostic_handler& dh)
  -> failure_or<std::string>;

/// Resolved token endpoint configuration.
struct resolved_token_endpoint {
  std::string url;
  std::vector<std::pair<std::string, std::string>> headers;
  /// Query parameters appended to `url`, percent-encoded at request time.
  std::vector<std::pair<std::string, std::string>> query_params;
  /// JSON path to extract the token from the endpoint response.
  /// nullopt means the response is plain text (no JSON parsing).
  Option<std::string> path;
};

/// Resolved path to a file holding the web identity token.
struct resolved_token_file {
  std::string path;
};

/// Resolved web identity token that was configured directly.
struct resolved_token {
  std::string token;
};

/// The resolved web identity token source. Exactly one source is configured;
/// `web_identity_options::from_record` enforces this.
using resolved_web_identity
  = variant<resolved_token_endpoint, resolved_token_file, resolved_token>;

/// Token endpoint configuration for fetching OIDC tokens via HTTP.
struct token_endpoint_options {
  Option<nova::Secret> url;
  Option<std::vector<std::pair<std::string, nova::Secret>>> headers;
  /// Query parameters to append to `url`, such as the `audience` that GitHub
  /// Actions and most other OIDC providers expect.
  Option<std::vector<std::pair<std::string, nova::Secret>>> query_params;
  /// JSON path to extract the token from endpoint response.
  /// Defaults to ".access_token". Set to null for plain text responses.
  Option<std::string> path;
  /// True if path was explicitly set to null (plain text response).
  bool path_is_null = false;
  location loc;

  /// Parses token endpoint options from an evaluated TQL record.
  static auto from_record(located<nova::Record> config, diagnostic_handler& dh)
    -> failure_or<token_endpoint_options>;
};

/// Web identity token configuration for OIDC-based authentication.
struct web_identity_options {
  Option<token_endpoint_options> token_endpoint;
  Option<nova::Secret> token_file;
  Option<nova::Secret> token;
  location loc;

  /// Parses web identity options from an evaluated TQL record.
  static auto from_record(located<nova::Record> config, diagnostic_handler& dh)
    -> failure_or<web_identity_options>;

  /// Reveals the configured token source for the SDK credential callbacks.
  auto resolve(diagnostic_handler& dh) const
    -> failure_or<resolved_web_identity>;
};

/// Fetches a web identity token from the configured source.
///
/// This blocks the calling thread; both the AWS and Azure SDKs invoke their
/// credential callbacks synchronously.
auto fetch_web_identity_token(const resolved_web_identity& web_identity)
  -> caf::expected<std::string>;

} // namespace tenzir
