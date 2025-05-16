//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/atoms.hpp"
#include "tenzir/variant.hpp"

#include <caf/error.hpp>
#include <caf/typed_actor.hpp>

#include <string>

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

struct secret_store_actor_traits {
  using signatures = caf::type_list<
    /// Resolve a secret.
    auto(atom::resolve, std::string name, std::string public_key)
      ->caf::result<secret_resolution_result>>;
};

using secret_store_actor = caf::typed_actor<secret_store_actor_traits>;

} // namespace tenzir
