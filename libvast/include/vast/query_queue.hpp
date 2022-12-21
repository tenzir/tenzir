//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/query_context.hpp"
#include "vast/system/actors.hpp"
#include "vast/uuid.hpp"

#include <vector>

namespace vast {

struct query_state {
  using type_query_context_map
    = std::unordered_map<vast::type, vast::query_context>;
  static constexpr bool use_deep_to_string_formatter = true;

  /// The query expression for each schema.
  type_query_context_map query_contexts_per_type;

  /// The query client.
  system::receiver_actor<atom::done> client = {};

  /// The number of partitions that need to be evaluated for this query.
  uint32_t candidate_partitions = 0;

  /// The number of partitions that have been reqested by the client.
  uint32_t requested_partitions = 0;

  /// The number of partitions that the query was sent to.
  uint32_t scheduled_partitions = 0;

  /// The number of partitions that are processed already.
  uint32_t completed_partitions = 0;

  template <class Inspector>
  friend auto inspect(Inspector& f, query_state& x) {
    return f.object(x)
      .pretty_name("query_state")
      .fields(f.field("query-contexts-per-type", x.query_contexts_per_type),
              f.field("client", x.client),
              f.field("candidate-partitions", x.candidate_partitions),
              f.field("requested-partitions", x.requested_partitions),
              f.field("scheduled-partitions", x.scheduled_partitions),
              f.field("completed-partitions", x.completed_partitions));
  }

  std::size_t memusage() const {
    auto total_query_context_memusage
      = std::accumulate(query_contexts_per_type.begin(),
                        query_contexts_per_type.end(), 0,
                        [](auto value, const auto& schema_context_entry) {
                          return value + schema_context_entry.second.memusage();
                        });
    return sizeof(*this) + total_query_context_memusage;
  }
};

class query_queue {
public:
  /// The entry type for the `partitions` lists. Maps a partition ID
  /// to a list of query IDs.
  struct entry {
    entry(uuid partition_id, type schema, uint64_t priority,
          std::vector<uuid> queries, bool erased)
      : partition{std::move(partition_id)},
        schema{std::move(schema)},
        priority{priority},
        queries{std::move(queries)},
        erased{erased} {
    }

    uuid partition;
    type schema;
    uint64_t priority = 0;
    std::vector<uuid> queries;
    bool erased = false;

    friend bool operator<(const entry& lhs, const entry& rhs) noexcept;
    friend bool operator==(const entry& lhs, const uuid& rhs) noexcept;

    std::size_t memusage() const;
  };

  // -- observers --------------------------------------------------------------

  /// Calculates the number of partitions that need to be loaded to complete all
  /// queries.
  [[nodiscard]] size_t num_partitions() const;

  /// Returns the number of currently queued queries.
  [[nodiscard]] size_t num_queries() const;

  /// Checks whether queries with outstanding work exist.
  [[nodiscard]] bool has_work() const;

  /// Checks whether the given query can be reached from the queue of
  /// partitions. Should only be used for assertions.
  [[nodiscard]] bool reachable(const uuid& qid) const;

  /// Creates an ID for a query and makes sure to avoid collisions with other
  /// existing query IDs.
  [[nodiscard]] uuid create_query_id() const;

  /// Retrieves a handle to the contained queries.
  [[nodiscard]] const std::unordered_map<uuid, query_state>& queries() const;

  // -- modifiers --------------------------------------------------------------

  /// Inserts a new query into the queue.
  [[nodiscard]] caf::error
  insert(query_state&& query_state, system::catalog_lookup_result&& candidates);

  /// Activates an inactive query.
  [[nodiscard]] caf::error activate(const uuid& qid, uint32_t num_partitions);

  /// Removes a query from the queue entirely.
  [[nodiscard]] caf::error remove_query(const uuid& qid);

  /// Removes a partition from the queue.
  bool mark_partition_erased(const uuid& pid);

  /// Retrieves the next partition to be scheduled and the related queries and
  /// increments the scheduled counters for the latter.
  [[nodiscard]] std::optional<entry> next();

  /// Returns a client handle in case the requested batch has been completed.
  [[nodiscard]] std::optional<system::receiver_actor<atom::done>>
  handle_completion(const uuid& qid);

  std::size_t memusage() const;

private:
  /// Maps query IDs to pending queries lookup state.
  std::unordered_map<uuid, query_state> queries_ = {};

  /// Maps partitions IDs to lists of query IDs.
  std::vector<entry> partitions = {};

  /// Maps partitions IDs to lists of query IDs, only contains entries where all
  /// queries are currently inactive.
  std::vector<entry> inactive_partitions = {};
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::query_queue::entry> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const vast::query_queue::entry& value, FormatContext& ctx) const {
    return format_to(ctx.out(), "(partition: {}; priority: {}; queries: {})",
                     value.partition, value.priority, value.queries);
  }
};

} // namespace fmt
