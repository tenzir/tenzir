//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/api/vast_api.h"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/logger.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/connect_to_node.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/fwd.hpp>
#include <caf/io/middleman.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

struct VAST {
  ~VAST() = default;

  //
  caf::detail::scope_guard<void (*)()> log_context;
  // We need to store the configuration separately, because of course the
  // actor_system only stores it by reference. (wtf)
  vast::system::configuration cfg;
  // TODO: We probably want an actor system similar to what is used in the
  // unit tests, that only moves when one of the API functions is called.
  caf::actor_system actor_system;
};

struct vast_connection {
  vast::system::node_actor node;
  std::string endpoint;
};

extern "C" struct VAST* vast_initialize() {
  auto* result = static_cast<struct VAST*>(::malloc(sizeof(VAST)));
  auto log_context
    = vast::create_log_context(vast::invocation{}, caf::settings{});
  if (!log_context)
    return nullptr;
  // caf::scope_guard doesn't have a move assignment operator
  new (&result->log_context)
    caf::detail::scope_guard<void (*)()>{std::move(*log_context)};
  new (&result->cfg) vast::system::configuration{};
  new (&result->actor_system) caf::actor_system{result->cfg};
  return result;
}

// FIXME: catch exceptions and return nullptr
extern "C" struct vast_connection*
vast_open(struct VAST* vast, const char* endpoint) {
  auto self = caf::scoped_actor{vast->actor_system};
  caf::settings vast_cfg;
  caf::put(vast_cfg, "vast.endpoint", endpoint);
  auto maybe_node = vast::system::connect_to_node(self, vast_cfg);
  if (!maybe_node) {
    VAST_WARN("{}", maybe_node.error());
    return nullptr;
  }
  self->monitor(*maybe_node);
  return new vast_connection{*maybe_node, endpoint};
}

extern "C" int vast_close(struct VAST* vast, struct vast_connection* conn) {
  auto self = caf::scoped_actor{vast->actor_system};
  self->send_exit(conn->node, caf::exit_reason::user_shutdown);
  caf::error error;
  self->receive([&](const caf::down_msg&) {},
                [&](caf::error e) { error = std::move(e); });
  if (error)
    return -1;
  return 0;
}

// Return 0 on success
extern "C" int vast_info(struct vast_info* out) {
  out->version = vast::version::version;
  return 0;
}

extern "C" int vast_status_json(struct VAST* vast, struct vast_connection* conn,
                                char* out, size_t n) {
  auto self = caf::scoped_actor{vast->actor_system};
  vast::invocation invocation;
  invocation.full_name = "status";
  caf::error err;
  self->send(conn->node, vast::atom::run_v, std::move(invocation));
  self->receive([&](const caf::down_msg&) { err = vast::ec::remote_node_down; },
                [&](const std::string& str) {
                  // Status messages or query results.
                  // TODO: store the whole string in `VAST`, so we can be
                  // resumable.
                  ::strncpy(out, str.c_str(), n);
                },
                [&](caf::error e) { err = std::move(e); });
  if (err)
    return -1;
  return 0;
}

// Closes the connection
extern "C" void vast_finalize(struct VAST* vast) {
  vast->~VAST();
  ::free(vast);
}