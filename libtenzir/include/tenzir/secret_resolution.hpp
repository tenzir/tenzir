//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/ecc.hpp"
#include "tenzir/location.hpp"
#include "tenzir/secret.hpp"

#include <string_view>

namespace arrow {
template <typename T>
class Result;
}

namespace tenzir {

/// @relates `operator_control_plane::resolve_secrets_must_yield`
class resolved_secret_value {
public:
  resolved_secret_value() = default;

  explicit resolved_secret_value(ecc::cleansing_blob value, bool all_literal
                                                            = false)
    : value_{std::move(value)}, all_literal_{all_literal} {
  }

  /// Returns a string view over the secret's UTF-8 value, if it is valid UTF-8.
  auto utf8_view() const -> std::optional<std::string_view>;

  /// Returns a string view over the secret's UTF-8 value, if it is valid UTF-8.
  /// Otherwise, emits a diagnostic::error
  auto utf8_view(std::string_view name, location loc,
                 diagnostic_handler& dh) const -> std::string_view;

  /// Whether the secret only consists of literals, i.e. is all plain text
  /// This is mostly useful for a decision to censor secrets
  auto all_literal() const -> bool {
    return all_literal_;
  }

  /// Returns a view over the secret's raw bytes.
  auto blob() const -> std::span<const std::byte> {
    return value_;
  }

  /// @brief Clears the secret value and scrubs the memory. This should be
  /// called on any `resolved_secret_value` objects that remain alive, but are
  /// no longer used.
  auto clear() {
    value_.clear();
    value_.shrink_to_fit();
  }

  /// A `resolved_secret_value` contains a plain text secret. It must not be
  /// serialized.
  friend auto inspect(auto&, resolved_secret_value&) = delete;

private:
  ecc::cleansing_blob value_;
  bool all_literal_ = false;
};

/// A utility that censors any occurrence of (part of) a secret in a string.
/// @relates make_secret_request
struct secret_censor {
public:
  auto censor(std::string text) const -> std::string;
  auto censor(const arrow::Status& status) const -> std::string;
  template <typename T>
  auto censor(const arrow::Result<T>& r) const -> std::string {
    return censor(r.status());
  }

  size_t max_size = 3;
  bool censor_literals = false;
  std::vector<resolved_secret_value> secrets;
};

using secret_request_callback = std::function<void(resolved_secret_value)>;
using record_request_callback
  = std::function<void(std::string_view, resolved_secret_value)>;

/// @relates `operator_control_plane::resolve_secrets`
struct secret_request {
  /// The secret to resolve
  class secret secret;
  /// The location associated with the secret
  struct location location;
  /// The callback to invoke once this secret is resolved
  secret_request_callback callback;

  /// A secret request that will invoke `callback` on successful resolution
  secret_request(tenzir::secret secret, tenzir::location loc,
                 secret_request_callback callback)
    : secret{std::move(secret)}, location{loc}, callback{std::move(callback)} {
  }

  /// A secret request that will invoke `callback` on successful resolution
  secret_request(const located<tenzir::secret>& secret,
                 secret_request_callback callback)
    : secret{secret.inner},
      location{secret.source},
      callback{std::move(callback)} {
  }

  /// A secret request that will directly set `out` on successful resolution
  secret_request(tenzir::secret secret, tenzir::location loc,
                 resolved_secret_value& out, secret_censor* censor = nullptr);

  /// A secret request that will directly set `out` on successful resolution
  secret_request(const located<tenzir::secret>& secret,
                 resolved_secret_value& out, secret_censor* censor = nullptr);
};

namespace detail {
/// Creates a secret_request_callback that sets `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view
auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   std::string& out, diagnostic_handler& dh,
                                   secret_censor* censor = nullptr)
  -> secret_request_callback;

/// Creates a secret_request_callback that sets `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view
auto secret_string_setter_callback(std::string name, tenzir::location loc,
                                   located<std::string>& out,
                                   diagnostic_handler& dh,
                                   secret_censor* censor = nullptr)
  -> secret_request_callback;
} // namespace detail

/// Creates a secret request that will set `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view
auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         std::string& out, diagnostic_handler& dh,
                         secret_censor* censor = nullptr) -> secret_request;

/// Creates a secret request that will set `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view

auto make_secret_request(std::string name, secret s, tenzir::location loc,
                         located<std::string>& out, diagnostic_handler& dh,
                         secret_censor* censor = nullptr) -> secret_request;

/// Creates a secret request that will set `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view
auto make_secret_request(std::string name, const located<secret>& s,
                         located<std::string>& out, diagnostic_handler& dh,
                         secret_censor* censor = nullptr) -> secret_request;

/// Creates a secret request that will set `out`, if the secret is a valid
/// UTF-8 string and raises an error otherwise.
/// @relates operator_control_plane::resolve_secrets_must_yield
/// @relates resolved_secret_value::utf8_view
auto make_secret_request(std::string name, const located<secret>& s,
                         std::string& out, diagnostic_handler& dh,
                         secret_censor* censor = nullptr) -> secret_request;
} // namespace tenzir
