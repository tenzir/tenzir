//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/retention_policy.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <unordered_set>
#include <vector>

namespace tenzir {

class importer {
public:
  static inline const char* name = "importer";

  explicit importer(importer_actor::pointer self, index_actor index);
  ~importer() noexcept;

  auto make_behavior() -> importer_actor::behavior_type;

private:
  void send_report();

  /// Process a slice and forward it to the index.
  void handle_slice(table_slice&& slice);

  void flush(Option<type> schema = {});
  void flush_nova();
  void finish_nova_flush();
  void release_snapshot_barrier(uuid token);

  /// Pointer to the owning actor.
  importer_actor::pointer self;

  detail::flat_map<type, uint64_t> schema_counters = {};
  std::unordered_map<std::string, uint64_t> nova_name_counters = {};

  /// The index actor and the policy for retention.
  index_actor index;
  struct retention_policy retention_policy = {};
  duration import_buffer_timeout = std::chrono::seconds{1};

  /// Buffered events waiting to be flushed.
  std::unordered_map<type, std::vector<table_slice>> unpersisted_events = {};
  std::vector<nova::Events> unpersisted_nova_events = {};
  size_t unpersisted_nova_bytes = 0;
  size_t max_unpersisted_nova_bytes = size_t{16} * 1024 * 1024;
  uint64_t nova_buffer_generation = 0;
  size_t pending_nova_requests = 0;
  caf::error nova_error = caf::none;
  std::vector<caf::typed_response_promise<void>> nova_flush_waiters = {};
  std::vector<caf::typed_response_promise<void>> nova_accept_waiters = {};

  struct subscriber {
    receiver_actor<table_slice> receiver;
    bool internal;
    bool eager;
  };

  /// A list of subscribers for incoming events.
  std::vector<subscriber> subscribers = {};
  struct nova_subscriber {
    receiver_actor<nova::Events> receiver;
    bool internal;
    bool eager;
  };
  std::vector<nova_subscriber> nova_subscribers = {};
  std::unordered_set<uuid> snapshot_barriers = {};
};

} // namespace tenzir
