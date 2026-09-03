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
#include "tenzir/option.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/secret_resolution.hpp"
#include "tenzir/variant.hpp"

#include <caf/expected.hpp>

#include <string>
#include <utility>
#include <vector>

namespace tenzir {

/// Assigns the `string` or `secret` at `key` to `x`, if the record has one.
auto assign_secret(const located<record>& config, std::string_view key,
                   Option<secret>& x, diagnostic_handler& dh)
  -> failure_or<void>;

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
  Option<secret> url;
  Option<std::vector<std::pair<std::string, secret>>> headers;
  /// Query parameters to append to `url`, such as the `audience` that GitHub
  /// Actions and most other OIDC providers expect.
  Option<std::vector<std::pair<std::string, secret>>> query_params;
  /// JSON path to extract the token from endpoint response.
  /// Defaults to ".access_token". Set to null for plain text responses.
  Option<std::string> path;
  /// True if path was explicitly set to null (plain text response).
  bool path_is_null = false;
  location loc;

  friend auto inspect(auto& f, token_endpoint_options& x) -> bool {
    return f.object(x).fields(
      f.field("url", x.url), f.field("headers", x.headers),
      f.field("query_params", x.query_params), f.field("path", x.path),
      f.field("path_is_null", x.path_is_null), f.field("loc", x.loc));
  }

  /// Parses token endpoint options from a TQL record.
  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<token_endpoint_options>;
};

/// Web identity token configuration for OIDC-based authentication.
struct web_identity_options {
  Option<token_endpoint_options> token_endpoint;
  Option<secret> token_file;
  Option<secret> token;
  location loc;

  friend auto inspect(auto& f, web_identity_options& x) -> bool {
    return f.object(x).fields(f.field("token_endpoint", x.token_endpoint),
                              f.field("token_file", x.token_file),
                              f.field("token", x.token), f.field("loc", x.loc));
  }

  /// Parses web identity options from a TQL record.
  static auto from_record(located<record> config, diagnostic_handler& dh)
    -> failure_or<web_identity_options>;

  /// Creates secret requests for resolving the token source.
  ///
  /// @param resolved Output variant to store the resolved token source. The
  ///                 requests write into its active alternative, so it must
  ///                 outlive them and must not be reassigned before they
  ///                 resolve.
  auto make_secret_requests(resolved_web_identity& resolved,
                            diagnostic_handler& dh) const
    -> std::vector<secret_request>;
};

/// Fetches a web identity token from the configured source.
///
/// This blocks the calling thread; both the AWS and Azure SDKs invoke their
/// credential callbacks synchronously.
auto fetch_web_identity_token(const resolved_web_identity& web_identity)
  -> caf::expected<std::string>;

} // namespace tenzir
