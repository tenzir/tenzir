//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/query_queue.hpp"

#include "vast/detail/algorithms.hpp"

namespace vast {

namespace {
std::size_t memusage(const std::vector<query_queue::entry>& entries) {
  return std::accumulate(cbegin(entries), cend(entries), std::size_t{0u},
                         [](const auto& accumulated, const auto& current) {
                           return accumulated + current.memusage();
                         });
}
} // namespace

bool operator<(const query_queue::entry& lhs,
               const query_queue::entry& rhs) noexcept {
  return lhs.priority < rhs.priority;
}

bool operator==(const query_queue::entry& lhs, const uuid& rhs) noexcept {
  return lhs.partition == rhs;
}

size_t query_queue::num_partitions() const {
  return partitions.size() + inactive_partitions.size();
}

size_t query_queue::num_queries() const {
  return queries_.size();
}

[[nodiscard]] bool query_queue::has_work() const {
  return !partitions.empty();
}

[[nodiscard]] bool query_queue::reachable(const uuid& qid) const {
  auto run = [&](const auto& ps) {
    return std::any_of(ps.begin(), ps.end(), [&](const auto& x) {
      return std::any_of(x.queries.begin(), x.queries.end(),
                         [&](const auto& q) {
                           return qid == q;
                         });
    });
  };
  return run(partitions) || run(inactive_partitions);
}

[[nodiscard]] uuid query_queue::create_query_id() const {
  auto query_id = uuid::random();
  // Ensure the query id is unique.
  while (queries_.find(query_id) != queries_.end())
    query_id = uuid::random();
  return query_id;
}

[[nodiscard]] const std::unordered_map<uuid, query_state>&
query_queue::queries() const {
  return queries_;
}

[[nodiscard]] caf::error
query_queue::insert(query_state&& query_state, std::vector<uuid>&& candidates) {
  if (candidates.empty())
    return caf::make_error(ec::unspecified, "can't add a query with 0 "
                                            "candidates");
  if (query_state.candidate_partitions != candidates.size())
    return caf::make_error(ec::unspecified, "the candidate set size must match "
                                            "the query state");
  auto qid = query_state.query_contexts_per_type.begin()->second.id;
  auto [query_state_it, emplace_success]
    = queries_.emplace(qid, std::move(query_state));
  if (!emplace_success)
    return caf::make_error(ec::unspecified, "A query with this ID exists "
                                            "already");
  for (const auto& cand : candidates) {
    auto it = std::find(partitions.begin(), partitions.end(), cand);
    if (it != partitions.end()) {
      it->priority += query_state_it->second.query_contexts_per_type.begin()
                        ->second.priority;
      it->queries.push_back(qid);
      VAST_ASSERT(!detail::contains(inactive_partitions, cand),
                  "A partition must not be active and inactive at the same "
                  "time");
      continue;
    }
    it
      = std::find(inactive_partitions.begin(), inactive_partitions.end(), cand);
    if (it != inactive_partitions.end()) {
      it->priority += query_state_it->second.query_contexts_per_type.begin()
                        ->second.priority;
      it->queries.push_back(qid);
      partitions.push_back(std::move(*it));

      inactive_partitions.erase(it);
      continue;
    }
    partitions.push_back(query_queue::entry{
      cand,
      query_state_it->second.query_contexts_per_type.begin()->second.priority,
      std::vector{qid}, false});
  }
  // TODO: Insertion sort should be better.
  std::sort(partitions.begin(), partitions.end());
  return caf::none;
}

[[nodiscard]] caf::error
query_queue::activate(const uuid& qid, uint32_t num_partitions) {
  auto it = queries_.find(qid);
  if (it == queries_.end())
    return caf::make_error(ec::unspecified, "cannot activate unknown query");
  it->second.requested_partitions += num_partitions;
  // Go over all currently inactive partitions and splice those relevant for
  // `qid` back into the active queue.
  auto new_inactive = std::vector<query_queue::entry>{};
  std::partition_copy(std::make_move_iterator(inactive_partitions.begin()),
                      std::make_move_iterator(inactive_partitions.end()),
                      std::back_inserter(partitions),
                      std::back_inserter(new_inactive), [&](const auto& p) {
                        return std::find(p.queries.begin(), p.queries.end(),
                                         qid)
                               != p.queries.end();
                      });
  inactive_partitions = std::move(new_inactive);
  std::sort(partitions.begin(), partitions.end());
  return caf::none;
}

[[nodiscard]] caf::error query_queue::remove_query(const uuid& qid) {
  VAST_TRACE("index removes query {}", qid);
  auto it = queries_.find(qid);
  if (it == queries_.end())
    return caf::make_error(ec::unspecified, "cannot remove unknown query");
  queries_.erase(it);
  auto run = [&](auto& queue) {
    auto it = queue.begin();
    while (it < queue.end()) {
      auto queries_it = std::find(it->queries.begin(), it->queries.end(), qid);
      if (queries_it == it->queries.end()) {
        ++it;
        continue;
      }
      it->queries.erase(queries_it);
      if (it->queries.empty())
        it = queue.erase(it);
      else
        ++it;
    }
  };
  run(partitions);
  run(inactive_partitions);
  return caf::none;
}

bool query_queue::mark_partition_erased(const uuid& pid) {
  auto it = std::find(partitions.begin(), partitions.end(), pid);
  if (it != partitions.end()) {
    it->erased = true;
    VAST_ASSERT_CHEAP(!detail::contains(inactive_partitions, pid),
                      "A partition must not be active and inactive at the same "
                      "time");
    return true;
  }
  it = std::find(inactive_partitions.begin(), inactive_partitions.end(), pid);
  if (it != inactive_partitions.end()) {
    it->erased = true;
    return true;
  }
  return false;
}

std::optional<query_queue::entry> query_queue::next() {
  while (!partitions.empty()) {
    auto result = std::move(partitions.back());
    partitions.pop_back();
    auto active = entry{result.partition, 0ull, {}, result.erased};
    auto inactive = entry{result.partition, 0ull, {}, result.erased};
    std::partition_copy(
      std::make_move_iterator(result.queries.begin()),
      std::make_move_iterator(result.queries.end()),
      std::back_inserter(active.queries), std::back_inserter(inactive.queries),
      [&](const auto& qid) {
        auto it = queries_.find(qid);
        if (it == queries_.end()) {
          VAST_WARN("index tried to access non-existent query {}", qid);
          // Consider it inactive.
          return false;
        }
        auto& query_state = it->second;
        return query_state.requested_partitions
               > query_state.scheduled_partitions;
      });
    if (!inactive.queries.empty()) {
      for (auto qid_it = inactive.queries.begin();
           qid_it != inactive.queries.end();) {
        auto it = queries_.find(*qid_it);
        if (it == queries_.end()) {
          // We must have already warned about this above, no need to repeat.
          qid_it = inactive.queries.erase(qid_it);
          continue;
        }
        inactive.priority
          += it->second.query_contexts_per_type.begin()->second.priority;
        ++qid_it;
      }
      inactive_partitions.push_back(std::move(inactive));
    }
    if (!active.queries.empty()) {
      for (const auto& qid : active.queries) {
        auto it = queries_.find(qid);
        if (it == queries_.end()) {
          VAST_WARN("index tried to access non-existent query {}", qid);
          continue;
        }
        it->second.scheduled_partitions++;
      }
      return active;
    }
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<system::receiver_actor<atom::done>>
query_queue::handle_completion(const uuid& qid) {
  auto it = queries_.find(qid);
  if (it == queries_.end()) {
    // Queries get removed from the queue when the client signals no more
    // interest.
    VAST_DEBUG("index tried to access non-existent query {}", qid);
    return std::nullopt;
  }
  auto result = std::optional<system::receiver_actor<atom::done>>{};
  auto& query_state = it->second;
  query_state.completed_partitions++;
  if (query_state.completed_partitions == query_state.requested_partitions)
    result = query_state.client;
  if (query_state.completed_partitions == query_state.candidate_partitions) {
    VAST_ASSERT(!reachable(qid));
    queries_.erase(qid);
  }
  return result;
}

std::size_t query_queue::entry::memusage() const {
  return sizeof(*this) + queries.size() * sizeof(decltype(queries)::value_type);
}

std::size_t query_queue::memusage() const {
  auto usage = std::size_t{sizeof(*this)};
  for (const auto& [uid, query_state] : queries_) {
    usage += sizeof(uid) + query_state.memusage();
  }
  return usage + vast::memusage(partitions)
         + vast::memusage(inactive_partitions);
}

} // namespace vast
