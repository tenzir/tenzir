//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/make_copy_on_write.hpp>

using namespace tenzir;

namespace {

/// A catalog state populated directly, without an actor. Which partitions the
/// budget loop would evict is a pure function of the resident synopses, which
/// is what these tests pin down.
struct fixture {
  catalog_state state;
  tenzir::time clock = tenzir::time{} + std::chrono::hours{1};

  fixture() {
    factory<synopsis>::initialize();
  }

  /// Adds a partition and returns its id. Each call advances the clock, so
  /// partitions are ordered oldest-first by insertion.
  auto add(std::string_view schema_name = "test", uint64_t bytes = 1000)
    -> uuid {
    const auto id = uuid::random();
    auto schema
      = type{std::string{schema_name}, record_type{{"msg", string_type{}}}};
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().schema = schema;
    synopsis.unshared().events = 100;
    synopsis.unshared().approx_bytes = bytes;
    clock += std::chrono::seconds{1};
    synopsis.unshared().min_import_time = clock;
    synopsis.unshared().max_import_time = clock;
    state.synopses_per_type[schema][id] = std::move(synopsis);
    return id;
  }

  /// Moves a partition into the deferred set, as an erasure behind a pin does.
  /// `bytes` is split across the partition's files, and `approx_bytes` is set
  /// to something far larger on purpose: the budget loop measures the disk,
  /// so it must not reach for the decoded-size estimate.
  void park(const uuid& id, uint64_t bytes) {
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().store_file.size = bytes / 2;
    synopsis.unshared().indexes_file.size = bytes / 4;
    synopsis.unshared().sketches_file.size = bytes - bytes / 2 - bytes / 4;
    synopsis.unshared().approx_bytes = bytes * 10;
    state.deferred[id] = deferred_erase{.synopsis = std::move(synopsis)};
  }
};

} // namespace

TEST("eviction takes the oldest partitions first") {
  auto f = fixture{};
  const auto first = f.add();
  const auto second = f.add();
  f.add();
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{first, second}));
}

TEST("eviction crosses schemas") {
  auto f = fixture{};
  const auto oldest = f.add("a");
  const auto next = f.add("b");
  f.add("a");
  // The budget loop reclaims space, so unlike a rebuild it has no reason to
  // keep a batch within one schema.
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{oldest, next}));
}

TEST("eviction skips a partition a transform is holding") {
  auto f = fixture{};
  const auto first = f.add();
  const auto second = f.add();
  const auto third = f.add();
  f.state.in_transformation.insert(first);
  // Erasing an input would let its data come back through the output.
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{second, third}));
}

TEST("eviction stops at the step size") {
  auto f = fixture{};
  f.add();
  f.add();
  f.add();
  CHECK_EQUAL(f.state.select_eviction_batch(1).size(), 1u);
  CHECK_EQUAL(f.state.select_eviction_batch(0).size(), 0u);
  // Asking for more than there is yields what there is.
  CHECK_EQUAL(f.state.select_eviction_batch(10).size(), 3u);
}

namespace {

/// A policy that weights one schema above everything else, to show that the
/// catalog orders by what the policy says rather than by age.
struct weighted_policy final : tenzir::storage_policy {
  std::string heavy = {};

  auto eviction_weight(const uuid&, const partition_synopsis& synopsis) const
    -> Option<double> override {
    if (synopsis.schema.name() != heavy) {
      return None{};
    }
    // Weighted age, the way the compaction policy expresses it: a weight is a
    // multiplier on the age, not a value on a scale of its own.
    const auto age = std::chrono::duration<double>{tenzir::time::clock::now()
                                                   - synopsis.max_import_time}
                       .count();
    return 1000.0 * age;
  }
};

} // namespace

TEST("a policy weight decides the eviction order") {
  auto f = fixture{};
  // Oldest first, so without a policy this one would go first.
  const auto oldest = f.add("light");
  const auto newest = f.add("heavy");
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{oldest});
  auto policy = std::make_unique<weighted_policy>();
  policy->heavy = "heavy";
  f.state.policy = std::move(policy);
  // The policy outweighs the age difference, so the younger one goes first.
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{newest});
}

TEST("a partition the policy has no weight for keeps its age") {
  auto f = fixture{};
  const auto oldest = f.add("light");
  f.add("light");
  auto policy = std::make_unique<weighted_policy>();
  policy->heavy = "absent";
  f.state.policy = std::move(policy);
  // `none` from the policy falls back to age, which is oldest-first.
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{oldest});
}

TEST("parked bytes count the partitions waiting on their pins") {
  auto f = fixture{};
  CHECK_EQUAL(f.state.parked_bytes(), 0u);
  f.park(uuid::random(), 700);
  f.park(uuid::random(), 300);
  // These are erased already and their files go once the last reader drops, so
  // the budget loop must not erase more data on their account. The count is
  // the on-disk footprint, not the decoded-size estimate.
  CHECK_EQUAL(f.state.parked_bytes(), 1000u);
}
