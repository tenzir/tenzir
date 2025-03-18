//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/enum.hpp"
#include "tenzir/variant.hpp"

#include <string>

#pragma once

namespace tenzir {

struct secret_resolution_error {
  std::string message;
  bool terminal = false;

  secret_resolution_error() = default;

  secret_resolution_error(std::string message, bool terminal)
    : message{std::move(message)}, terminal{terminal} {
  }

  secret_resolution_error(caf::error err)
    : message{err.what()}, terminal{true} {
  }

  friend auto inspect(auto& f, secret_resolution_error& x) {
    return f.object(x)
      .pretty_name("secret_resolution_error")
      .fields(f.field("message", x.message), f.field("terminal", x.terminal));
  }
};

struct encrypted_secret_value {
  std::string value;

  ~encrypted_secret_value();

  friend auto inspect(auto& f, encrypted_secret_value& x) {
    return f.object(x)
      .pretty_name("encrypted_secret_value")
      .fields(f.field("value", x.value));
  }
};

/// @brief the kind of value this secret holds. This currently only exists for
/// future extensibility.
TENZIR_ENUM(secret_value_type, string);

class resolved_secret_value {
public:
  resolved_secret_value() = default;

  resolved_secret_value(std::string value) : value_{std::move(value)} {
  }

  auto clear_view() const -> std::string_view {
    return value_;
  }

  auto value_type() const -> secret_value_type {
    return value_type_;
  }

  ~resolved_secret_value() {
    clear();
  }

  /// @brief Clears the secret value and scrubs the memory. This should be
  /// called on any `resolved_secret_value` objects that remain alive, but are
  /// no longer used.
  auto clear() -> void;

private:
  secret_value_type value_type_ = secret_value_type::string;
  std::string value_;
};

struct secret_resolution_result
  : variant<encrypted_secret_value, secret_resolution_error> {
  using super = variant<encrypted_secret_value, secret_resolution_error>;
  using super::super;
};

} // namespace tenzir
