//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "web/fwd.hpp"

#include "web/fbs/server_state.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/flatbuffer.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::web {

struct token_description {
  std::string name;
  std::chrono::system_clock::time_point issued_at;
  std::chrono::system_clock::time_point expires_at;
  token_t token;
};

struct authenticator_state {
  static constexpr inline auto name = "authenticator";

  // Member functions

  authenticator_state() = default;

  caf::expected<token_t> generate();

  [[nodiscard]] bool authenticate(token_t) const;

  caf::error initialize_from(chunk_ptr);

  caf::expected<chunk_ptr> save();

  // Data members

  /// Filesystem path of the authenticator state, relative to state directory.
  std::filesystem::path path_ = {};

  /// Handle of the filesystem actor.
  filesystem_actor filesystem_ = {};

  /// The list of all known authentication tokens.
  std::vector<token_description> tokens_ = {};
};

caf::expected<authenticator_actor>
get_authenticator(caf::scoped_actor&, node_actor node, caf::timespan timeout);

/// Spawns the AUTHENTICATOR.
authenticator_actor::behavior_type
authenticator(authenticator_actor::stateful_pointer<authenticator_state> self,
              filesystem_actor fs);

} // namespace tenzir::plugins::web
