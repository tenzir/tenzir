//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/offset_commit.hpp"

#include <tenzir/test/test.hpp>

#include <cstdint>
#include <initializer_list>

namespace tenzir::plugins::kafka {

namespace {

constexpr auto ok = RdKafka::ERR_NO_ERROR;

TEST("a clean commit retires the offset") {
  CHECK(commit_outcome(ok, ok) == CommitOutcome::committed);
}

TEST("an offset this consumer can never commit is dropped") {
  // Keeping any of these is what poisons the consumer: the partition rejoins
  // every later commit and fails it, so no partition ever commits again.
  for (auto err : {
         RdKafka::ERR_UNKNOWN_MEMBER_ID,
         RdKafka::ERR_ILLEGAL_GENERATION,
         RdKafka::ERR_UNKNOWN_TOPIC_OR_PART,
         RdKafka::ERR__UNKNOWN_PARTITION,
         RdKafka::ERR__STATE,
         RdKafka::ERR_GROUP_AUTHORIZATION_FAILED,
       }) {
    CHECK(commit_outcome(err, err) == CommitOutcome::drop);
    CHECK(not is_retryable_commit_error(err));
  }
}

TEST("a coordinator that is unavailable or moving is retried") {
  for (auto err : {
         RdKafka::ERR__WAIT_COORD,
         RdKafka::ERR_COORDINATOR_LOAD_IN_PROGRESS,
         RdKafka::ERR_COORDINATOR_NOT_AVAILABLE,
         RdKafka::ERR_NOT_COORDINATOR,
         RdKafka::ERR_REBALANCE_IN_PROGRESS,
       }) {
    CHECK(commit_outcome(err, err) == CommitOutcome::retry);
    CHECK(is_retryable_commit_error(err));
  }
}

TEST("a request that never reached the broker is retried") {
  // These arrive as a top-level code only, with every list entry still
  // carrying its initial `ERR_NO_ERROR`. Reading that as success would retire
  // offsets that were never stored.
  for (auto err : {
         RdKafka::ERR__TIMED_OUT,
         RdKafka::ERR__TRANSPORT,
         RdKafka::ERR__ALL_BROKERS_DOWN,
         RdKafka::ERR_REQUEST_TIMED_OUT,
       }) {
    CHECK(commit_outcome(err, ok) == CommitOutcome::retry);
    CHECK(is_retryable_commit_error(err));
  }
}

TEST("an unblamed partition is retried rather than presumed either way") {
  // The call failed, but the broker did not blame this partition. It either
  // committed while a sibling was blamed, or nothing was judged at all. The
  // two are indistinguishable, and a repeat is safe under both: storing an
  // offset is idempotent, and the blamed partitions are gone by then.
  CHECK(commit_outcome(RdKafka::ERR_UNKNOWN_MEMBER_ID, ok)
        == CommitOutcome::retry);
  CHECK(commit_outcome(RdKafka::ERR_UNKNOWN_TOPIC_OR_PART, ok)
        == CommitOutcome::retry);
}

TEST("a partial response retires the partitions it did not blame") {
  // `ERR__PARTIAL` is the one top-level failure that still means every entry
  // was judged, so silence about a partition is success rather than an open
  // question. Retrying it instead would hold the offset pending behind a
  // sibling that keeps failing, and keep the partition out of
  // `committed_partitions` for as long as that lasts.
  CHECK(commit_outcome(RdKafka::ERR__PARTIAL, ok) == CommitOutcome::committed);
  // The per-partition verdict still outranks it in both directions.
  CHECK(commit_outcome(RdKafka::ERR__PARTIAL, RdKafka::ERR_UNKNOWN_MEMBER_ID)
        == CommitOutcome::drop);
  CHECK(
    commit_outcome(RdKafka::ERR__PARTIAL, RdKafka::ERR_REBALANCE_IN_PROGRESS)
    == CommitOutcome::retry);
  // A partial response is not itself worth repeating: the blamed partitions
  // carry their own verdict and the rest are done.
  CHECK(not is_retryable_commit_error(RdKafka::ERR__PARTIAL));
}

TEST("a partial failure with a stuck sibling still retires the good offset") {
  // The scenario from the review: one partition commits, another keeps
  // failing retryably. The good offset must not wait for the bad one.
  auto const overall = RdKafka::ERR__PARTIAL;
  CHECK(commit_outcome(overall, ok) == CommitOutcome::committed);
  CHECK(commit_outcome(overall, RdKafka::ERR_COORDINATOR_LOAD_IN_PROGRESS)
        == CommitOutcome::retry);
}

TEST("a per-partition verdict outranks the code of the call") {
  // Retryable call, doomed partition.
  CHECK(commit_outcome(RdKafka::ERR_REBALANCE_IN_PROGRESS,
                       RdKafka::ERR_UNKNOWN_MEMBER_ID)
        == CommitOutcome::drop);
  // Doomed call, retryable partition.
  CHECK(commit_outcome(RdKafka::ERR_UNKNOWN_MEMBER_ID,
                       RdKafka::ERR_REBALANCE_IN_PROGRESS)
        == CommitOutcome::retry);
  // A successful call cannot mask a partition that failed.
  CHECK(commit_outcome(ok, RdKafka::ERR_UNKNOWN_MEMBER_ID)
        == CommitOutcome::drop);
  CHECK(commit_outcome(ok, RdKafka::ERR_REBALANCE_IN_PROGRESS)
        == CommitOutcome::retry);
}

TEST("a partial failure converges in one retry") {
  // The scenario the whole design turns on: two partitions in one call, one of
  // them permanently dead. The first attempt drops the dead one and retries
  // the other; the retry no longer carries the dead partition, so it commits.
  auto const overall = RdKafka::ERR_UNKNOWN_MEMBER_ID;
  CHECK(commit_outcome(overall, RdKafka::ERR_UNKNOWN_MEMBER_ID)
        == CommitOutcome::drop);
  CHECK(commit_outcome(overall, ok) == CommitOutcome::retry);
  CHECK(commit_outcome(ok, ok) == CommitOutcome::committed);
}

auto tp(int32_t partition) -> TopicPartition {
  return TopicPartition{"events", partition};
}

auto pending(std::initializer_list<int32_t> partitions)
  -> TopicPartitionOffsets {
  auto result = TopicPartitionOffsets{};
  for (auto partition : partitions) {
    result.emplace(tp(partition), 100 + partition);
  }
  return result;
}

auto owned(std::initializer_list<int32_t> partitions) -> TopicPartitionSet {
  auto result = TopicPartitionSet{};
  for (auto partition : partitions) {
    result.insert(tp(partition));
  }
  return result;
}

TEST("only owned partitions are committed") {
  CHECK_EQUAL(committable_partitions(pending({0, 1, 2}), owned({0, 2})),
              owned({0, 2}));
}

TEST("an unreadable assignment commits everything") {
  // Failing to read the assignment must not stall offset progress.
  CHECK_EQUAL(committable_partitions(pending({0, 1}), None{}), owned({0, 1}));
}

TEST("an empty assignment commits nothing") {
  // Mid-rebalance under the eager protocol. The offsets stay pending, so a
  // partition that comes back is still committed by a later call.
  CHECK(committable_partitions(pending({0, 1}), TopicPartitionSet{}).empty());
}

TEST("a foreign partition is never submitted") {
  // The case that matters: this member fetched partition 1 before a rebalance
  // handed it to someone else. Committing it now would overwrite the new
  // owner's progress, because the broker accepts the write.
  auto const got = committable_partitions(pending({0, 1}), owned({0}));
  CHECK_EQUAL(got, owned({0}));
  CHECK(not got.contains(tp(1)));
}

} // namespace

} // namespace tenzir::plugins::kafka
