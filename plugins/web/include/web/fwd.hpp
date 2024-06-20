//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/actors.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

#include <memory>

namespace tenzir::plugins::web {

class restinio_response;
using restinio_response_ptr = std::shared_ptr<restinio_response>;

using token_t = std::string;

} // namespace tenzir::plugins::web

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_web_plugin_types, 1500)
  CAF_ADD_TYPE_ID(tenzir_web_plugin_types,
                  (std::shared_ptr<tenzir::plugins::web::restinio_response>))

  CAF_ADD_ATOM(tenzir_web_plugin_types, tenzir::atom, generate, "generate")
  CAF_ADD_ATOM(tenzir_web_plugin_types, tenzir::atom, validate, "validate")
CAF_END_TYPE_ID_BLOCK(tenzir_web_plugin_types)

namespace tenzir::plugins::web {

/// Server-side AUTHENTICATOR actor.
using authenticator_actor = typed_actor_fwd<
  // Generate a token.
  auto(atom::generate)->caf::result<token_t>,
  // Validate a token.
  auto(atom::validate, token_t)->caf::result<bool>>
  // Conform to the protocol of a STATUS CLIENT actor.
  ::extend_with<component_plugin_actor>::unwrap;

} // namespace tenzir::plugins::web

CAF_BEGIN_TYPE_ID_BLOCK(tenzir_web_plugin_actors, tenzir_web_plugin_types::end)
  CAF_ADD_TYPE_ID(tenzir_web_plugin_actors,
                  (tenzir::plugins::web::authenticator_actor))
CAF_END_TYPE_ID_BLOCK(tenzir_web_plugin_actors)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(
  std::shared_ptr<tenzir::plugins::web::restinio_response>)
