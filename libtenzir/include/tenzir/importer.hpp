//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/retention_policy.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

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

  void flush(std::optional<type> schema = {});

  /// Pointer to the owning actor.
  importer_actor::pointer self;

  detail::flat_map<type, uint64_t> schema_counters = {};

  /// The index actor and the policy for retention.
  index_actor index;
  struct retention_policy retention_policy = {};
  duration import_buffer_timeout = std::chrono::seconds{1};

  /// Buffered events waiting to be flushed.
  std::unordered_map<type, std::vector<table_slice>> unpersisted_events = {};

  /// A list of subscribers for incoming events.
  std::vector<std::pair<receiver_actor<table_slice>, bool /*internal*/>>
    subscribers = {};
};

} // namespace tenzir
