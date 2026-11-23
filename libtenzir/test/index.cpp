// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("rejected appends release active reservations") {
  auto state = index_state{nullptr};
  auto key = ImportShapeKey{"events", false, {"value"}};
  auto generation = uuid::random();
  auto kept = state.retain_nova_batch(300);
  auto rejected = state.retain_nova_batch(200);
  state.nova_active_partitions.emplace(
    key, nova_active_partition_info{{}, 5, 500, generation, {kept, rejected}});
  state.buffered_events = 5;
  state.rollback_nova_append(key, generation, 2, 200, rejected);
  CHECK_EQUAL(state.nova_active_partitions.at(key).events, 3u);
  CHECK_EQUAL(state.nova_active_partitions.at(key).bytes, 300u);
  CHECK_EQUAL(state.buffered_events, 3u);
  CHECK_EQUAL(state.buffered_nova_bytes, 300u);
}

TEST("rejected appends do not charge a replacement generation") {
  auto state = index_state{nullptr};
  auto key = ImportShapeKey{"events", false, {"value"}};
  auto generation = uuid::random();
  auto replacement = uuid::random();
  auto kept = state.retain_nova_batch(300);
  auto rejected = state.retain_nova_batch(200);
  state.nova_active_partitions.emplace(
    key, nova_active_partition_info{{}, 3, 300, replacement, {kept}});
  state.nova_unpersisted.emplace(generation,
                                 index_state::nova_unpersisted_partition_info{
                                   .bytes = 200, .batches = {rejected}});
  state.releasing_nova_bytes = 200;
  state.buffered_events = 3;
  state.rollback_nova_append(key, generation, 2, 200, rejected);
  CHECK_EQUAL(state.nova_active_partitions.at(key).events, 3u);
  CHECK_EQUAL(state.nova_active_partitions.at(key).bytes, 300u);
  CHECK_EQUAL(state.nova_unpersisted.at(generation).bytes, 0u);
  CHECK_EQUAL(state.buffered_events, 3u);
  CHECK_EQUAL(state.buffered_nova_bytes, 300u);
  state.nova_unpersisted.erase(generation);
  state.rollback_nova_append(key, generation, 2, 200, rejected);
  CHECK_EQUAL(state.buffered_events, 3u);
  CHECK_EQUAL(state.buffered_nova_bytes, 300u);
}

TEST("a batch shared by partitions counts once until its last holder") {
  auto state = index_state{nullptr};
  auto lhs = ImportShapeKey{"events", false, {"a"}};
  auto rhs = ImportShapeKey{"events", false, {"b"}};
  auto lhs_generation = uuid::random();
  auto rhs_generation = uuid::random();
  auto batch = state.retain_nova_batch(1000);
  state.nova_active_partitions.emplace(
    lhs, nova_active_partition_info{{}, 3, 300, lhs_generation, {batch}});
  state.nova_active_partitions.emplace(
    rhs, nova_active_partition_info{{}, 7, 700, rhs_generation, {batch}});
  state.nova_retained_batches.at(batch).holders += 2;
  state.buffered_events = 10;
  state.release_nova_batch(batch);
  CHECK_EQUAL(state.buffered_nova_bytes, 1000u);
  state.rollback_nova_append(lhs, lhs_generation, 3, 300, batch);
  CHECK_EQUAL(state.buffered_nova_bytes, 1000u);
  state.rollback_nova_append(rhs, rhs_generation, 7, 700, batch);
  CHECK_EQUAL(state.buffered_nova_bytes, 0u);
  CHECK(state.nova_retained_batches.empty());
}

TEST("publication errors do not leak into later waves") {
  auto state = index_state{nullptr};
  state.complete_publication(caf::make_error(ec::logic_error, "failed"));
  CHECK(not state.publication_error.valid());
  state.complete_publication(caf::none);
  CHECK(not state.publication_error.valid());
}
