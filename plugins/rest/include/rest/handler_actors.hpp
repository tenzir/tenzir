//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/system/query_cursor.hpp>

#include <caf/typed_event_based_actor.hpp>
#include <rest/fwd.hpp>
#include <restinio/all.hpp>

namespace vast::plugins::rest {

using restinio_request = std::shared_ptr<
  restinio::generic_request_t<restinio::no_extra_data_factory_t::data_t>>;

struct request {
  restinio_request impl;
};

struct status_handler_state {
  system::node_actor node_;
  std::vector<request> pending_;
};

struct export_handler_state {
  export_handler_state() = default;
  system::index_actor index_ = {};
  std::optional<system::query_cursor> cursor_ = std::nullopt;
  std::string body_ = {};
  // std::vector<request> pending_ = {};
};

/// Spawn a STATUS_HANDLER actor.
caf::behavior status_handler(caf::stateful_actor<status_handler_state>* self,
                             system::node_actor node);

/// Spawn an EXPORT_HANDLER actor.
export_handler_actor::behavior_type
export_handler(export_handler_actor::stateful_pointer<export_handler_state> self,
               system::index_actor index);

} // namespace vast::plugins::rest
