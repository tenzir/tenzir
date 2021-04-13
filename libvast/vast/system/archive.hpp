//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/segment_store.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/instrumentation.hpp"

#include <caf/actor_addr.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace vast::system {

/// @relates archive
struct archive_state {
  enum class operation { //
    count,
    erase,
    extract,
    extract_with_ids
  };

  struct request_state {
    request_state(receiver_actor<table_slice> sink_, operation op_,
                  expression expr_,
                  std::pair<ids, caf::typed_response_promise<atom::done>> ids_)
      : sink{std::move(sink_)}, op{op_}, expr{std::move(expr_)} {
      ids_queue.push(std::move(ids_));
    }
    receiver_actor<table_slice> sink;
    operation op;
    expression expr;
    std::queue<std::pair<ids, caf::typed_response_promise<atom::done>>>
      ids_queue;
    bool cancelled = false;
  };

  std::deque<request_state> requests;
  std::unique_ptr<vast::segment_store::lookup> session;
  ids session_ids = {};
  caf::typed_response_promise<atom::done> active_promise;

  archive_actor::pointer self;

  std::unique_ptr<vast::segment_store> store;

  void send_report();
  vast::system::measurement measurement;
  accountant_actor accountant;
  static inline const char* name = "archive";
  const uint64_t partition_offset = 0;
};

/// Stores event batches and answers queries for ID sets.
/// @param self The actor handle.
/// @param dir The root directory of the archive.
/// @param capacity The number of segments to cache in memory.
/// @param max_segment_size The maximum segment size in bytes.
/// @pre `max_segment_size > 0`
archive_actor::behavior_type
archive(archive_actor::stateful_pointer<archive_state> self,
        const std::filesystem::path& dir, size_t capacity,
        size_t max_segment_size);

} // namespace vast::system
