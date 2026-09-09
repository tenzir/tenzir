//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "kafka/eof_tracker.hpp"

#include <tenzir/test/test.hpp>

#include <cstdint>
#include <initializer_list>

namespace tenzir::plugins::kafka {

namespace {

constexpr auto topic = "events";

auto tp(int32_t partition) -> TopicPartition {
  return TopicPartition{topic, partition};
}

auto tps(std::initializer_list<int32_t> partitions) -> TopicPartitionSet {
  auto result = TopicPartitionSet{};
  for (auto partition : partitions) {
    result.insert(tp(partition));
  }
  return result;
}

TEST("a fresh tracker never finishes") {
  auto tracker = EofTracker{};
  CHECK(not tracker.finished());
  CHECK(tracker.assigned().empty());
}

TEST("the run finishes once every assigned partition reached its end") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  CHECK(tracker.mark_eof(tp(1)));
  CHECK(tracker.finished());
}

TEST("an empty assignment finishes immediately") {
  // More parallel instances than the topic has partitions: the broker assigns
  // this one nothing, and there is nothing for it to read.
  auto tracker = EofTracker{};
  CHECK(tracker.assign({}));
  CHECK(tracker.finished());
}

TEST("an eager revoke does not finish the run") {
  // The regression this guards: the eager rebalance protocol revokes every
  // partition before assigning the new set, so the assignment is transiently
  // empty. Reading that as "done" truncates a healthy run whenever another
  // instance joins the consumer group.
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  tracker.revoke({});
  CHECK(not tracker.finished());
  // The assign that follows restores the truthful assignment.
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  CHECK(tracker.mark_eof(tp(1)));
}

TEST("a revoke that takes every partition away still needs an assign") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0})));
  tracker.revoke({});
  CHECK(not tracker.finished());
  // Only the assign may conclude, even though it hands back nothing.
  CHECK(tracker.assign({}));
}

TEST("a stale re-read never concludes from an empty assignment") {
  // The lazy refresh path: an EOF arrived for a partition outside the known
  // assignment, so the view gets re-read. The consumer may be mid-rebalance at
  // that moment, and an empty read there is no evidence that the run is over.
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0})));
  CHECK(not tracker.resync({}));
  CHECK(not tracker.finished());
  // The stale read is discarded rather than adopted, so the known assignment
  // survives and the partition's end still counts.
  CHECK(tracker.assigned().contains(tp(0)));
  CHECK(tracker.mark_eof(tp(0)));
}

TEST("a stale re-read adopts a non-empty assignment") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0})));
  // Partition 1 turned up without us seeing the rebalance frame yet.
  CHECK(not tracker.resync(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  CHECK(tracker.mark_eof(tp(1)));
}

TEST("a shrinking assignment can complete the eof set") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  // Partition 1 moved to another member; partition 0 is at its end already.
  CHECK(tracker.assign(tps({0})));
}

TEST("eof marks for foreign partitions are ignored") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0})));
  CHECK(not tracker.mark_eof(tp(7)));
  CHECK(not tracker.finished());
  CHECK(tracker.mark_eof(tp(0)));
}

TEST("eof marks are dropped for partitions that move away") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(1)));
  // Partition 1 leaves and comes back, so its end has to be observed again.
  CHECK(not tracker.assign(tps({0})));
  CHECK(not tracker.assign(tps({0, 1})));
  CHECK(not tracker.mark_eof(tp(0)));
  CHECK(tracker.mark_eof(tp(1)));
}

TEST("clear resets the tracker for a new subscription") {
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0})));
  CHECK(tracker.mark_eof(tp(0)));
  tracker.clear();
  CHECK(not tracker.finished());
  CHECK(tracker.assigned().empty());
}

auto observed(AssignmentChange kind, uint64_t generation)
  -> ObservedAssignmentChange {
  return ObservedAssignmentChange{.kind = kind, .generation = generation};
}

TEST("a current assign is authoritative") {
  CHECK_EQUAL(to_assignment_refresh(observed(AssignmentChange::assigned, 4), 4),
              AssignmentRefresh::assigned);
}

TEST("an assign superseded by a later rebalance is only opportunistic") {
  // The event described generation 4, but the consumer has since moved to 5,
  // so the assignment read now is not the one the event announced.
  CHECK_EQUAL(to_assignment_refresh(observed(AssignmentChange::assigned, 4), 5),
              AssignmentRefresh::stale);
}

TEST("a revoke stays a revoke whether or not it was superseded") {
  CHECK_EQUAL(to_assignment_refresh(observed(AssignmentChange::revoked, 4), 4),
              AssignmentRefresh::revoked);
  CHECK_EQUAL(to_assignment_refresh(observed(AssignmentChange::revoked, 4), 9),
              AssignmentRefresh::revoked);
}

TEST("a superseded assign cannot conclude a run mid-rebalance") {
  // The scenario the generation check exists for: an assign of {0,1} is
  // queued, then a rebalance revokes everything before the operator gets to
  // act on it. Treating the event as authoritative would adopt the empty
  // assignment the consumer holds *now* and finish a run that has read
  // nothing.
  auto tracker = EofTracker{};
  CHECK(not tracker.assign(tps({0, 1})));
  auto const queued = observed(AssignmentChange::assigned, 4);
  auto const why = to_assignment_refresh(queued, 5);
  REQUIRE_EQUAL(why, AssignmentRefresh::stale);
  // The assignment is transiently empty under the eager protocol.
  CHECK(not tracker.resync(TopicPartitionSet{}));
  CHECK(not tracker.finished());
  // Had it been trusted, the run would have ended here.
  CHECK(tracker.assign(TopicPartitionSet{}));
}

} // namespace

} // namespace tenzir::plugins::kafka
