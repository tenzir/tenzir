//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ecc.hpp"
#include "tenzir/variant.hpp"

#include <string>

#pragma once

namespace tenzir {

struct secret_resolution_error {
  std::string message;

  secret_resolution_error() = default;

  secret_resolution_error(std::string message) : message{std::move(message)} {
  }

  secret_resolution_error(caf::error err) : message{err.what()} {
  }

  friend auto inspect(auto& f, secret_resolution_error& x) {
    return f.apply(x.message);
  }
};

struct encrypted_secret_value {
  std::string value;

  friend auto inspect(auto& f, encrypted_secret_value& x) {
    return f.apply(x.value);
  }
};

struct secret_resolution_result
  : variant<encrypted_secret_value, secret_resolution_error> {
  using super = variant<encrypted_secret_value, secret_resolution_error>;
  using super::super;
};

class resolved_secret_value {
public:
  resolved_secret_value() = default;

  explicit resolved_secret_value(const ecc::cleansing_string& value)
    : value_{value} {
  }
  explicit resolved_secret_value(const ecc::cleansing_vector<std::byte>& value)
    : value_{value} {
  }

  resolved_secret_value(const resolved_secret_value&) = delete;

  ~resolved_secret_value() {
    clear();
  }

  const auto& value() const {
    return value_;
  }

  /// @brief Clears the secret value and scrubs the memory. This should be
  /// called on any `resolved_secret_value` objects that remain alive, but are
  /// no longer used.
  auto clear() -> void;

  friend auto inspect(auto&, resolved_secret_value&) = delete;

private:
  variant<ecc::cleansing_string, ecc::cleansing_vector<std::byte>> value_;
};

} // namespace tenzir
