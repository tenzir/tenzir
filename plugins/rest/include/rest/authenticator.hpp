//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "rest/fwd.hpp"

#include "rest/fbs/server_state.hpp"

#include <vast/flatbuffer.hpp>
#include <vast/system/actors.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest {

struct token_description {
  std::string name;
  std::chrono::system_clock::time_point issued_at;
  std::chrono::system_clock::time_point expires_at;
  token_t token;
};

struct authenticator_state {
  static constexpr auto name = "authenticator";

  authenticator_state() = default;

  token_t generate();

  [[nodiscard]] bool authenticate(token_t) const;

  caf::error initialize_from(chunk_ptr);

  caf::expected<chunk_ptr> save();

  std::filesystem::path path_ = {};
  system::filesystem_actor filesystem_ = {};
  std::vector<token_description> tokens_ = {};
};

caf::expected<authenticator_actor>
get_authenticator(caf::scoped_actor&, system::node_actor node,
                  caf::duration timeout);

/// Spawns the AUTHENTICATOR.
authenticator_actor::behavior_type
authenticator(authenticator_actor::stateful_pointer<authenticator_state> self,
              system::filesystem_actor fs);

} // namespace vast::plugins::rest
