// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("rejected appends release active reservations") {
  auto state = index_state{nullptr};
  auto key = ImportShapeKey{"events", false, {"value"}};
  auto generation = uuid::random();
  state.nova_active_partitions.emplace(
    key, nova_active_partition_info{{}, 5, 500, generation});
  state.buffered_events = 5;
  state.buffered_nova_bytes = 500;
  state.rollback_nova_append(key, generation, 2, 200);
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
  state.nova_active_partitions.emplace(
    key, nova_active_partition_info{{}, 3, 300, replacement});
  state.nova_unpersisted.emplace(
    generation, index_state::nova_unpersisted_partition_info{.bytes = 200});
  state.buffered_events = 3;
  state.buffered_nova_bytes = 500;
  state.rollback_nova_append(key, generation, 2, 200);
  CHECK_EQUAL(state.nova_active_partitions.at(key).events, 3u);
  CHECK_EQUAL(state.nova_active_partitions.at(key).bytes, 300u);
  CHECK_EQUAL(state.nova_unpersisted.at(generation).bytes, 0u);
  CHECK_EQUAL(state.buffered_events, 3u);
  CHECK_EQUAL(state.buffered_nova_bytes, 300u);
  state.nova_unpersisted.erase(generation);
  state.rollback_nova_append(key, generation, 2, 200);
  CHECK_EQUAL(state.buffered_events, 3u);
  CHECK_EQUAL(state.buffered_nova_bytes, 300u);
}

TEST("publication errors do not leak into later waves") {
  auto state = index_state{nullptr};
  state.complete_publication(caf::make_error(ec::logic_error, "failed"));
  CHECK(not state.publication_error.valid());
  state.complete_publication(caf::none);
  CHECK(not state.publication_error.valid());
}
