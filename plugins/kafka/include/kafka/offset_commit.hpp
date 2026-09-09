//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "kafka/eof_tracker.hpp"

#include <tenzir/option.hpp>

#include <librdkafka/rdkafkacpp.h>

#include <cstdint>

namespace tenzir::plugins::kafka {

/// What to do with one pending offset after an attempt to commit it.
enum class CommitOutcome : uint8_t {
  /// The broker stored the offset. Stop tracking it.
  committed,
  /// The outcome is unknown or may improve. Keep the offset and try again.
  retry,
  /// This consumer can never commit the offset. Stop tracking it.
  drop,
};

/// Returns whether an offset commit is worth repeating after this error.
///
/// The retryable set is the one where the offset is still this consumer's to
/// commit: the coordinator is unavailable, moving, or busy, or the request
/// never reached it. Everything else means the group moved on without us — the
/// partition belongs to another member, the broker fenced us, or the topic is
/// gone.
constexpr auto is_retryable_commit_error(RdKafka::ErrorCode err) -> bool {
  switch (err) {
    case RdKafka::ERR__WAIT_COORD:
    case RdKafka::ERR__TIMED_OUT:
    case RdKafka::ERR__TRANSPORT:
    case RdKafka::ERR__ALL_BROKERS_DOWN:
    case RdKafka::ERR_COORDINATOR_LOAD_IN_PROGRESS:
    case RdKafka::ERR_COORDINATOR_NOT_AVAILABLE:
    case RdKafka::ERR_NOT_COORDINATOR:
    case RdKafka::ERR_REBALANCE_IN_PROGRESS:
    case RdKafka::ERR_REQUEST_TIMED_OUT:
      return true;
    default:
      return false;
  }
}

/// Decides the fate of one partition's offset after a commit attempt.
///
/// librdkafka reports both a top-level code and a per-partition code in the
/// list it was handed. A commit of several partitions can fail for only some
/// of them, and the top-level code then carries one of the failures while the
/// partitions that did commit keep a code of `ERR_NO_ERROR`. Judging the whole
/// batch by the top-level code would hold those offsets back forever, and —
/// because a partition that can never be committed keeps failing every call it
/// joins — would stall offset commits for the entire consumer from the first
/// such partition onwards.
///
/// So the per-partition code decides whenever the broker set one. When it did
/// not, the top-level code says how much to read into that silence:
///
/// - `ERR_NO_ERROR` means the whole call succeeded.
/// - `ERR__PARTIAL` means the broker answered and judged every partition, and
///   only some were blamed. An unblamed partition therefore did commit. It has
///   to be retired here, because a sibling that keeps failing retryably would
///   otherwise hold it pending for the whole retry budget and keep it out of
///   `committed_partitions`, which is what tells a consumer with an explicit
///   `offset` that this run has already made progress on the partition.
/// - Anything else means the call as a whole did not land, so nothing was
///   judged. Retrying is safe: storing an offset is idempotent, and the caller
///   drops the blamed partitions before retrying, so the repeat can succeed
///   where the original could not.
constexpr auto commit_outcome(RdKafka::ErrorCode overall,
                              RdKafka::ErrorCode partition) -> CommitOutcome {
  if (partition != RdKafka::ERR_NO_ERROR) {
    return is_retryable_commit_error(partition) ? CommitOutcome::retry
                                                : CommitOutcome::drop;
  }
  if (overall == RdKafka::ERR_NO_ERROR or overall == RdKafka::ERR__PARTIAL) {
    return CommitOutcome::committed;
  }
  return CommitOutcome::retry;
}

/// Selects the pending offsets that this consumer may currently commit.
///
/// A commit is only this member's to make while it owns the partition. The
/// broker does not enforce that: an offset submitted for a foreign partition
/// is accepted under this member's own generation and overwrites the progress
/// of whoever owns the partition now.
///
/// `owned` is the assignment as read at the moment of the commit, or `None`
/// when it could not be read at all. An unreadable assignment submits
/// everything, as before, rather than stalling progress on a failed query.
///
/// Unowned partitions are omitted, not erased. Under the eager rebalance
/// protocol the assignment is transiently empty while a rebalance is in
/// flight, and a partition that comes back is still this member's to commit.
/// Whatever does not come back is discarded once the next authoritative
/// assignment arrives.
inline auto committable_partitions(TopicPartitionOffsets const& pending,
                                   Option<TopicPartitionSet> const& owned)
  -> TopicPartitionSet {
  auto result = TopicPartitionSet{};
  for (auto const& [partition, offset] : pending) {
    if (owned and not owned->contains(partition)) {
      continue;
    }
    result.insert(partition);
  }
  return result;
}

} // namespace tenzir::plugins::kafka
