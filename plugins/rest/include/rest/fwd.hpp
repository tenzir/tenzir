//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>
#include <vast/http_api.hpp>
#include <vast/plugin.hpp>
#include <vast/system/actors.hpp>

#include <memory>

// CAF_BEGIN_TYPE_ID_BLOCK(vast_rest_plugin_types, 1500)

//   CAF_ADD_ATOM(vast_rest_plugin_types, vast::atom, request, "request")
//   CAF_ADD_ATOM(vast_rest_plugin_types, vast::atom, next, "next")

// CAF_END_TYPE_ID_BLOCK(vast_rest_plugin_types)

namespace vast::plugins::rest {

class restinio_response;

// using status_handler_actor = system::typed_actor_fwd<>
//   ::extend_with<system::rest_handler_actor>
//   ::unwrap;

// /// An actor to help with handling a single query.
// using export_helper_actor = system::typed_actor_fwd<
//     caf::reacts_to<atom::request, atom::query, http_request>,
//     caf::reacts_to<atom::request, atom::query, atom::next, http_request>,
//     caf::reacts_to<system::query_cursor>,
//     caf::reacts_to<atom::done>
//   >
//   ::extend_with<system::receiver_actor<table_slice>>
//   ::unwrap;

// /// An actor to receive REST endpoint requests and spawn exporters
// /// as needed.
// using export_multiplexer_actor = system::typed_actor_fwd<>
//   ::extend_with<system::rest_handler_actor>
//   ::unwrap;

} // namespace vast::plugins::rest

// CAF_BEGIN_TYPE_ID_BLOCK(vast_rest_plugin_actors, vast_rest_plugin_types::end)
//   CAF_ADD_TYPE_ID(vast_rest_plugin_actors,
//                   (vast::plugins::rest::export_helper_actor))
//   CAF_ADD_TYPE_ID(vast_rest_plugin_actors,
//                   (vast::plugins::rest::export_multiplexer_actor))
// CAF_END_TYPE_ID_BLOCK(vast_rest_plugin_actors)

// FIXME
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::plugins::rest::restinio_response);
