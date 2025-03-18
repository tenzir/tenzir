//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin.hpp"
#include "tenzir/secret_resolution.hpp"

namespace tenzir {

using secret_store_actor = typed_actor_fwd<
  /// Resolve a secret.
  auto(atom::resolve, std::string name, std::string public_key)
    ->caf::result<secret_resolution_result>>::unwrap;

} // namespace tenzir
