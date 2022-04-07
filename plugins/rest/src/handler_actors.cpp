//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/handler_actors.hpp"

#include <vast/command.hpp>
#include <vast/table_slice.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

namespace vast::plugins::rest {

static void abort_request(request& rq, const caf::error& e) {
  auto rsp = rq.impl->create_response(restinio::status_internal_server_error());
  rsp.set_body(caf::to_string(e));
  rsp.connection_close();
  rsp.done();
}

// We use a dynamic actor for the `/status` endpoint (possibly some others in
// the future?), because that is not exposed as a request/response API but only
// by sending an `invocation` to the node and getting back the result as a
// string in an unrelated message.
caf::behavior status_handler(caf::stateful_actor<status_handler_state>* self,
                             system::node_actor node) {
  self->state.node_ = std::move(node);
  return {
    [self](atom::request, atom::status, request rq) {
      self->state.pending_.push_back(std::move(rq));
      auto inv = vast::invocation{
        .options = {},
        .full_name = "status",
        .arguments = {},
      };
      self->send(self->state.node_, atom::run_v, std::move(inv));
    },
    [self](const caf::down_msg&) {
      for (auto& rq : std::exchange(self->state.pending_, {})) {
        auto rsp = rq.impl->create_response();
        rsp.header().status_code(restinio::http_status_code_t{500});
        rsp.set_body("node down");
        rsp.done();
      }
    },
    [self](const std::string& str) {
      for (auto& rq : std::exchange(self->state.pending_, {})) {
        auto rsp = rq.impl->create_response();
        rsp.header().add_field("Content-Type", "application/json");
        rsp.set_body(str);
        rsp.done();
      }
    },
    [self](const caf::error& e) {
      for (auto& rq : std::exchange(self->state.pending_, {})) {
        abort_request(rq, e);
      }
    },
  };
}

export_handler_actor::behavior_type
export_handler(export_handler_actor::stateful_pointer<export_handler_state> self,
               system::index_actor index) {
  self->state.index_ = std::move(index);
  return {
    [self](atom::request, atom::query, const request& rq) {
      auto rsp = rq.impl->create_response();
      rsp.set_body(self->state.body_);
      rsp.done();
    },
    [self](atom::request, atom::query, atom::next, request rq) {
      self->state.body_.clear();
      if (!self->state.cursor_) {
        abort_request(rq, caf::make_error(ec::invalid_query, "not yet ready"));
        return;
      }
      self->send(self->state.index_, self->state.cursor_->id, uint32_t{1});
    },
    [self](system::query_cursor cursor) {
      self->state.cursor_ = cursor;
    },
    [self](const vast::table_slice& slice) {
      self->state.body_ += fmt::format("{}\n", slice);
    },
    [self](atom::done) {
      // TODO: It would be better to wait until we get the `done`
      // to ensure the client gets a complete result back from
      // one GET request, but for some obscure reason the final
      // `done` doesn't seem to arrive here from the query supervisor.
      //
      // self->state.loading = false;
      // for (auto& rq : std::exchange(self->state.pending, {})) {
      //   auto rsp = rq.impl->create_response();
      //   rsp.set_body(self->state.body_);
      //   rsp.done();
      // }
    }};
}

} // namespace vast::plugins::rest
