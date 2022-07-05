//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/ids.hpp"
#include "vast/query_context.hpp"
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
  struct request_state {
    request_state(vast::query_context query_context,
                  std::pair<ids, caf::typed_response_promise<uint64_t>> ids_)
      : query_context{std::move(query_context)} {
      ids_queue.push(std::move(ids_));
    }
    vast::query_context query_context;
    std::queue<std::pair<ids, caf::typed_response_promise<uint64_t>>> ids_queue;
    uint64_t num_hits = 0;
    bool cancelled = false;
  };

  std::deque<request_state> requests;
  std::unique_ptr<vast::segment_store::lookup> session;
  ids session_ids = {};
  caf::typed_response_promise<uint64_t> active_promise;

  archive_actor::pointer self;

  std::unique_ptr<vast::segment_store> store;

  /// Send metrics to the accountant.
  void send_report();

  /// Opens the next lookup session with the segment_store, either
  /// by popping the next ids item from the queue of the current request
  /// or by moving to the next request.
  std::unique_ptr<segment_store::lookup> next_session();

  /// Updates an existing request with additional ids or inserts a new request
  /// if the query client hasn't been seen before.
  /// @param query The type of request.
  caf::typed_response_promise<uint64_t>
  file_request(vast::query_context query_context);

  vast::system::measurement measurement;
  accountant_actor accountant;
  static inline const char* name = "archive";
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
