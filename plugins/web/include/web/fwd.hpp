//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/actors.hpp>
#include <vast/fwd.hpp>
#include <vast/plugin.hpp>

#include <memory>

namespace vast::plugins::web {

class restinio_response;
using restinio_response_ptr = std::shared_ptr<restinio_response>;

using token_t = std::string;

} // namespace vast::plugins::web

CAF_BEGIN_TYPE_ID_BLOCK(vast_web_plugin_types, 1500)
  CAF_ADD_TYPE_ID(vast_web_plugin_types,
                  (std::shared_ptr<vast::plugins::web::restinio_response>))

  CAF_ADD_ATOM(vast_web_plugin_types, vast::atom, generate, "generate")
  CAF_ADD_ATOM(vast_web_plugin_types, vast::atom, validate, "validate")
CAF_END_TYPE_ID_BLOCK(vast_web_plugin_types)

namespace vast::plugins::web {

/// Server-side AUTHENTICATOR actor.
using authenticator_actor = typed_actor_fwd<
  // Generate a token.
  auto(atom::generate)->caf::result<token_t>,
  // Validate a token.
  auto(atom::validate, token_t)->caf::result<bool>>
  // Conform to the protocol of a STATUS CLIENT actor.
  ::extend_with<status_client_actor>::unwrap;

} // namespace vast::plugins::web

CAF_BEGIN_TYPE_ID_BLOCK(vast_web_plugin_actors, vast_web_plugin_types::end)
  CAF_ADD_TYPE_ID(vast_web_plugin_actors,
                  (vast::plugins::web::authenticator_actor))
CAF_END_TYPE_ID_BLOCK(vast_web_plugin_actors)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(
  std::shared_ptr<vast::plugins::web::restinio_response>)
