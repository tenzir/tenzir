//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ecc.hpp"
#include "tenzir/location.hpp"
#include "tenzir/secret.hpp"

#include <optional>
#include <span>
#include <string_view>

#pragma once

namespace tenzir {

/// @relates `operator_control_plane::resolve_secrets_must_yield`
class resolved_secret_value {
public:
  resolved_secret_value() = default;

  explicit resolved_secret_value(ecc::cleansing_blob value)
    : value_{std::move(value)} {
  }

  ~resolved_secret_value() = default;
  resolved_secret_value(const resolved_secret_value&) = delete;
  auto operator=(const resolved_secret_value&)
    -> resolved_secret_value& = delete;
  resolved_secret_value(resolved_secret_value&&) = default;
  auto operator=(resolved_secret_value&&) -> resolved_secret_value& = default;

  /// Returns a string view over the secret's UTF-8 value, if it is valid UTF-8.
  auto utf8_view() const -> std::optional<std::string_view>;

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
};

/// @relates `operator_control_plane::resolve_secrets_must_yield`
struct secret_request {
  tenzir::secret secret;
  resolved_secret_value& out;
  location loc = location::unknown;

  secret_request(tenzir::secret secret, resolved_secret_value& out,
                 location loc)
    : secret{std::move(secret)}, out{out}, loc{loc} {
  }

  secret_request(const located<tenzir::secret>& secret,
                 resolved_secret_value& out)
    : secret{secret.inner}, out{out}, loc{secret.source} {
  }
};

} // namespace tenzir
