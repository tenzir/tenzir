//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "kafka/librdkafka_utils.hpp"

#include <tenzir/hash/hash.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace tenzir::plugins::kafka {

/// Identifies one Kafka partition; hashable for direct use as container key.
struct TopicPartition {
  std::string topic;
  int32_t partition = -1;

  auto operator==(TopicPartition const&) const -> bool = default;

  friend auto inspect(auto& f, TopicPartition& x) -> bool {
    return f.object(x).fields(f.field("topic", x.topic),
                              f.field("partition", x.partition));
  }
};

struct TopicPartitionHash {
  auto operator()(TopicPartition const& x) const -> size_t {
    return tenzir::hash(x.topic, x.partition);
  }
};

using TopicPartitionSet
  = std::unordered_set<TopicPartition, TopicPartitionHash>;
using TopicPartitionOffsets
  = std::unordered_map<TopicPartition, int64_t, TopicPartitionHash>;

/// One rebalance event, together with the assignment it described.
///
/// The generation identifies the assignment in force when the event was
/// observed. An event is queued behind the data fetched with it, so it is
/// acted upon later than it was seen, by which time the generation may have
/// moved on.
struct ObservedAssignmentChange {
  AssignmentChange kind = AssignmentChange::assigned;
  uint64_t generation = 0;

  auto operator==(ObservedAssignmentChange const&) const -> bool = default;
};

/// Why the operator is re-reading the consumer's partition assignment.
enum class AssignmentRefresh : uint8_t {
  /// The broker assigned a new set of partitions. It is authoritative: it may
  /// conclude an `exit` run, even when empty, and it may evict pending
  /// offsets.
  assigned,
  /// The broker revoked partitions and has not assigned the new set yet.
  /// Nothing may be concluded from it, and no offset may be evicted.
  revoked,
  /// Our cached view of the assignment looked stale, so we re-read it
  /// opportunistically. The consumer may equally be mid-rebalance, so an empty
  /// result proves nothing and pending offsets stay untouched.
  stale,
};

/// Translates a rebalance event into the refresh it warrants.
///
/// `current` is the generation in force at the point of use. The assignment is
/// always re-read live, so an event whose generation has been superseded
/// describes a set the consumer no longer holds: acting on it as an assign
/// would let a rebalance that has *since* emptied the assignment conclude the
/// run. Such an event is demoted to an opportunistic read.
///
/// A revoke is not demoted, because it only withdraws trust and can never
/// conclude anything. Deferring cannot stall either: the generation moved, so
/// the event that moved it is still to be observed, and it carries the assign.
inline auto to_assignment_refresh(ObservedAssignmentChange change,
                                  uint64_t current) -> AssignmentRefresh {
  switch (change.kind) {
    case AssignmentChange::revoked:
      return AssignmentRefresh::revoked;
    case AssignmentChange::assigned:
      return change.generation == current ? AssignmentRefresh::assigned
                                          : AssignmentRefresh::stale;
  }
  TENZIR_UNREACHABLE();
}

/// Decides when a `from_kafka exit=true` run has read every partition it owns.
///
/// The run is over once every assigned partition reported `PARTITION_EOF`. The
/// subtlety is that the assignment moves underneath the consumer: whenever a
/// member joins or leaves the group, the broker rebalances. Under the eager
/// protocol — librdkafka's default `partition.assignment.strategy` — a
/// rebalance first revokes *every* partition and only then assigns the new
/// set, so the assignment is transiently empty while the run is far from over.
///
/// Reading "no assigned partitions, no outstanding EOFs" as "done" would
/// therefore truncate a healthy run every time another instance joins the
/// group. This is why the tracker only concludes on `AssignmentChange`
/// `assigned`: an assign always follows a revoke, and librdkafka invokes the
/// rebalance callback for it even when the new set is empty, so deferring
/// cannot stall the run.
///
/// A genuinely empty assignment — more parallel instances than the topic has
/// partitions — still finishes immediately, because it arrives as an assign.
class EofTracker {
public:
  /// Records a new assignment and reports whether the run may finish.
  ///
  /// EOF marks for partitions that are no longer assigned are dropped: they
  /// belong to another member now. Marks for partitions that survive the
  /// rebalance are kept, because librdkafka emits `PARTITION_EOF` when the
  /// fetch position reaches the end rather than continuously while parked
  /// there, so a dropped mark may never be delivered again.
  auto assign(TopicPartitionSet partitions) -> bool {
    assigned_ = std::move(partitions);
    std::erase_if(eof_, [this](TopicPartition const& partition) {
      return not assigned_.contains(partition);
    });
    assigned_seen_ = true;
    // A shrinking assignment can complete the EOF set without a new EOF event.
    return finished();
  }

  /// Adopts an opportunistically re-read assignment.
  ///
  /// Used when an EOF arrives for a partition outside the known assignment:
  /// the cached view is stale, but the consumer may equally be mid-rebalance,
  /// so an empty result is no evidence that the run is over. Such a result is
  /// discarded and the next assign corrects the view.
  auto resync(TopicPartitionSet partitions) -> bool {
    if (partitions.empty()) {
      return false;
    }
    return assign(std::move(partitions));
  }

  /// Records that the broker revoked partitions without assigning a new set.
  ///
  /// The assignment this leaves behind is not one to draw conclusions from,
  /// so the tracker only prunes the partitions it no longer owns and waits
  /// for the matching assign.
  auto revoke(TopicPartitionSet partitions) -> void {
    assigned_ = std::move(partitions);
    std::erase_if(eof_, [this](TopicPartition const& partition) {
      return not assigned_.contains(partition);
    });
    assigned_seen_ = false;
  }

  /// Records a `PARTITION_EOF` and reports whether the run may finish.
  ///
  /// EOFs for partitions this member does not own are ignored; they are stale
  /// events from before a rebalance.
  auto mark_eof(TopicPartition const& partition) -> bool {
    if (not assigned_.contains(partition)) {
      return false;
    }
    eof_.insert(partition);
    return finished();
  }

  /// Returns the partitions this member currently owns.
  auto assigned() const -> TopicPartitionSet const& {
    return assigned_;
  }

  /// Returns whether every assigned partition reached its end.
  ///
  /// Always false while a revoke is outstanding, however the sets compare.
  auto finished() const -> bool {
    return assigned_seen_ and eof_.size() == assigned_.size();
  }

  /// Resets the tracker for a new subscription.
  auto clear() -> void {
    assigned_.clear();
    eof_.clear();
    assigned_seen_ = false;
  }

private:
  TopicPartitionSet assigned_;
  /// Invariant: contains only partitions from `assigned_`.
  TopicPartitionSet eof_;
  /// Whether the current assignment comes from an assign rather than a revoke.
  bool assigned_seen_ = false;
};

} // namespace tenzir::plugins::kafka
