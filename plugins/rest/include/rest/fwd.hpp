//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/fwd.hpp>
#include <vast/system/actors.hpp>

#include <memory>

CAF_BEGIN_TYPE_ID_BLOCK(vast_compaction_plugin_types, 1400)

  CAF_ADD_ATOM(vast_compaction_plugin_types, vast::atom, request, "request")
  CAF_ADD_ATOM(vast_compaction_plugin_types, vast::atom, query, "query")
  CAF_ADD_ATOM(vast_compaction_plugin_types, vast::atom, next, "next")

CAF_END_TYPE_ID_BLOCK(vast_compaction_plugin_types)

namespace vast::plugins::rest {

struct request;

/// In principle we'd like an actor like this to handle generic REST api
/// calls by dispatching a request into the actor system and completing
/// the http response with the result.
/// Currently unused because the initial `/status` and `/export`
/// sample endpoints both don't use request-response style
/// messaging)
// using rest_handler_actor = system::typed_actor_fwd<
//   caf::reacts_to<atom::request, request>>::unwrap;

/// An actor to handle export requests.
using export_handler_actor = system::typed_actor_fwd<
  caf::reacts_to<atom::request, atom::query, request>,
  caf::reacts_to<atom::request, atom::query, atom::next, request>,
  caf::reacts_to<system::query_cursor>, caf::reacts_to<atom::done>>::
  extend_with<system::receiver_actor<table_slice>>::unwrap;

} // namespace vast::plugins::rest

CAF_BEGIN_TYPE_ID_BLOCK(vast_compaction_plugin_actors,
                        vast_compaction_plugin_types::end)
  CAF_ADD_TYPE_ID(vast_compaction_plugin_types, (vast::plugins::rest::request))
  // CAF_ADD_TYPE_ID(vast_compaction_plugin_actors,
  //                 (vast::plugins::rest::rest_handler_actor))
CAF_END_TYPE_ID_BLOCK(vast_compaction_plugin_actors)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::plugins::rest::request)
