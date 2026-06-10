//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/aliases.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/variant.hpp"

#include <caf/error.hpp>
#include <caf/typed_actor.hpp>

#include <string>

namespace tenzir {

struct authentication_resolution_error {
  std::string message;

  authentication_resolution_error() = default;

  authentication_resolution_error(std::string message)
    : message{std::move(message)} {
  }

  authentication_resolution_error(caf::error err) : message{err.what()} {
  }

  friend auto inspect(auto& f, authentication_resolution_error& x) {
    return f.apply(x.message);
  }
};

/// The platform-side authentication payload before any secret field has been
/// decrypted. Cleartext config fields (e.g. `client_id`, `token_url`,
/// `scopes`) live in `public_config`; the strategy-specific secret fields
/// (e.g. `client_secret`, `password`, `token`) appear in
/// `encrypted_secret_fields`, each value being a base58-encoded ECIES
/// ciphertext that the requesting node decrypts with the private half of the
/// transport key it sent along with the request.
struct encrypted_authentication_value {
  std::string strategy;
  record public_config;
  detail::stable_map<std::string, std::string> encrypted_secret_fields;

  friend auto inspect(auto& f, encrypted_authentication_value& x) {
    return f.object(x)
      .pretty_name("encrypted_authentication_value")
      .fields(f.field("strategy", x.strategy),
              f.field("public_config", x.public_config),
              f.field("encrypted_secret_fields", x.encrypted_secret_fields));
  }
};

struct authentication_resolution_result
  : variant<encrypted_authentication_value, authentication_resolution_error> {
  using super
    = variant<encrypted_authentication_value, authentication_resolution_error>;
  using super::super;
};

/// Result of a successful authentication resolution after key exchange and
/// decryption: a flat record containing both the cleartext `public_config`
/// fields and the decrypted secret fields, plus the strategy discriminator.
/// Consumers feed this through the same `parse_*_options` path the local YAML
/// auth entries take.
struct resolved_authentication {
  std::string strategy;
  record fields;
};

struct authentication_store_actor_traits {
  using signatures = caf::type_list<
    /// Resolve a named authentication.
    auto(atom::resolve_authentication, std::string name,
         std::string public_key)
      ->caf::result<authentication_resolution_result>>;
};

using authentication_store_actor
  = caf::typed_actor<authentication_store_actor_traits>;

} // namespace tenzir
