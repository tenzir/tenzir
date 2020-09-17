/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/chunk.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/expression.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/meta_index.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/fwd.hpp"
#include "vast/meta_index.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/msgpack_table_slice_builder.hpp"
#include "vast/span.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#define SUITE flatbuffers
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

using vast::byte;
using vast::span;

TEST(uuid roundtrip) {
  vast::uuid uuid = vast::uuid::random();
  auto expected_fb = vast::fbs::wrap(uuid);
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  vast::uuid uuid2 = vast::uuid::random();
  CHECK_NOT_EQUAL(uuid, uuid2);
  span<const byte> span{reinterpret_cast<const byte*>(fb->data()), fb->size()};
  vast::fbs::unwrap<vast::fbs::UUID>(span, uuid2);
  CHECK_EQUAL(uuid, uuid2);
}

TEST(meta index roundtrip) {
  // Prepare a mini meta index. The meta index only looks at the layout of the
  // table slices it gets, so we feed it with an empty table slice.
  auto meta_idx = vast::meta_index{};
  auto mock_partition = vast::uuid::random();
  vast::table_slice_header header;
  header.layout = vast::record_type{{"x", vast::count_type{}}}.name("y");
  auto slice = vast::msgpack_table_slice::make(header);
  REQUIRE(slice);
  meta_idx.add(mock_partition, *slice);
  // Serialize meta index.
  auto expected_fb = vast::fbs::wrap(meta_idx);
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  span<const byte> span{reinterpret_cast<const byte*>(fb->data()), fb->size()};
  // Deserialize meta index.
  vast::meta_index recovered_meta_idx;
  vast::fbs::unwrap<vast::fbs::MetaIndex>(span, recovered_meta_idx);
  // Check that lookups still work as expected.
  auto candidates = recovered_meta_idx.lookup(vast::expression{
    vast::predicate{vast::field_extractor{".x"}, vast::equal, vast::data{0u}},
  });
  REQUIRE_EQUAL(candidates.size(), 1u);
  CHECK_EQUAL(candidates[0], mock_partition);
}

TEST(index roundtrip) {
  vast::system::index_state state(/*self = */ nullptr);
  // The active partition is not supposed to appear in the
  // created flatbuffer
  state.active_partition.id = vast::uuid::random();
  // Both unpersisted and persisted partitions should show up in the created
  // flatbuffer.
  state.unpersisted[vast::uuid::random()] = nullptr;
  state.unpersisted[vast::uuid::random()] = nullptr;
  state.persisted_partitions.insert(vast::uuid::random());
  state.persisted_partitions.insert(vast::uuid::random());
  std::set<vast::uuid> expected_uuids;
  for (auto& kv : state.unpersisted)
    expected_uuids.insert(kv.first);
  for (auto& uuid : state.persisted_partitions)
    expected_uuids.insert(uuid);
  // Add some fake statistics
  state.stats.layouts["zeek.conn"] = vast::system::layout_statistics{54931u};
  // Serialize the index.
  auto expected_fb = vast::fbs::wrap(state, "I000");
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  auto span = as_bytes(fb);
  // Deserialize the index.
  auto idx = vast::fbs::GetIndex(span.data());
  // Check Index state.
  CHECK_EQUAL(idx->version(), vast::fbs::Version::v0);
  // We only check the presence and not the contents of the meta index
  // since that should be covered by the previous unit test.
  auto meta_idx = idx->meta_index();
  CHECK(meta_idx);
  auto partition_uuids = idx->partitions();
  REQUIRE(partition_uuids);
  CHECK_EQUAL(partition_uuids->size(), expected_uuids.size());
  std::set<vast::uuid> restored_uuids;
  for (auto uuid : *partition_uuids) {
    REQUIRE(uuid);
    vast::uuid restored_uuid;
    vast::unpack(*uuid, restored_uuid);
    restored_uuids.insert(restored_uuid);
  }
  CHECK_EQUAL(expected_uuids, restored_uuids);
  // Check that layout statistics were restored correctly
  auto stats = idx->stats();
  REQUIRE(stats);
  REQUIRE_EQUAL(stats->size(), 1u);
  REQUIRE(stats->Get(0));
  CHECK_EQUAL(stats->Get(0)->name()->str(), std::string{"zeek.conn"});
  CHECK_EQUAL(stats->Get(0)->count(), 54931u);
}

TEST(empty partition roundtrip) {
  // Create partition state.
  vast::system::active_partition_state state;
  state.id = vast::uuid::random();
  state.offset = 17;
  state.events = 23;
  state.combined_layout
    = vast::record_type{{"x", vast::count_type{}}}.name("y");
  auto& ids = state.type_ids["x"];
  ids.append_bits(0, 3);
  ids.append_bits(1, 3);
  // Serialize partition.
  auto expected_fb = vast::fbs::wrap(state);
  REQUIRE(expected_fb);
  auto span = as_bytes(*expected_fb);
  // Deserialize partition.
  vast::system::passive_partition_state readonly_state;
  auto partition = vast::fbs::GetPartition(span.data());
  REQUIRE(partition);
  auto error = unpack(*partition, readonly_state);
  CHECK(!error);
  std::cerr << caf::to_string(error) << std::endl;
  CHECK_EQUAL(readonly_state.id, state.id);
  CHECK_EQUAL(readonly_state.offset, state.offset);
  CHECK_EQUAL(readonly_state.events, state.events);
  CHECK_EQUAL(readonly_state.combined_layout, state.combined_layout);
  CHECK_EQUAL(readonly_state.type_ids, state.type_ids);
}

FIXTURE_SCOPE(foo, fixtures::deterministic_actor_system)

// This test spawns a partition, fills it with some test data, then persists
// the partition to disk, restores it from the persisted on-disk state, and
// finally does some queries on it to ensure the restored flatbuffer is still
// able to return correct results.
TEST(full partition roundtrip) {
  // Spawn a partition.
  auto fs = self->spawn(
    vast::system::posix_filesystem,
    directory); // `directory` is provided by the unit test fixture
  auto partition_uuid = vast::uuid::random();
  auto partition = sys.spawn(vast::system::active_partition, partition_uuid, fs,
                             caf::settings{});
  run();
  REQUIRE(partition);
  // Add data to the partition.
  auto layout = vast::record_type{{"x", vast::count_type{}}}.name("y");
  vast::msgpack_table_slice_builder builder(layout);
  CHECK(builder.add(0u));
  auto slice = builder.finish();
  auto data = std::vector<vast::table_slice_ptr>{slice};
  auto src = vast::detail::spawn_container_source(sys, data, partition);
  REQUIRE(src);
  run();
  // Persist the partition to disk;
  vast::path persist_path = "test-partition"; // will be interpreted relative to
                                              // the fs actor's root dir
  auto persist_promise = self->request(partition, caf::infinite,
                                       vast::atom::persist_v, persist_path);
  run();
  persist_promise.receive([](vast::atom::ok) { CHECK("persisting done"); },
                          [](caf::error err) { FAIL(err); });
  self->send_exit(partition, caf::exit_reason::user_shutdown);
  // Spawn a read-only partition from this chunk and try to query the data we
  // added. We make two queries, one "#type"-query and one "normal" query
  auto readonly_partition = sys.spawn(vast::system::passive_partition,
                                      partition_uuid, fs, persist_path);
  REQUIRE(readonly_partition);
  run();
  auto test_expression = [&](const vast::expression& expression,
                             size_t expected_indexers, size_t expected_ids) {
    auto rp = self->request(readonly_partition, caf::infinite, expression);
    run();
    rp.receive(
      [&](vast::evaluation_triples triples) {
        CHECK_EQUAL(triples.size(), expected_indexers);
        for (auto triple : triples) {
          auto curried_predicate = get<1>(triple);
          auto actor = get<2>(triple);
          CHECK(actor);
          auto rp = self->request(actor, caf::infinite, curried_predicate);
          run();
          rp.receive(
            [&](vast::ids ids) { CHECK_EQUAL(rank(ids), expected_ids); },
            [](caf::error) { CHECK(false); });
        }
      },
      [](caf::error) { CHECK(false); });
  };
  auto x_equals_zero = vast::expression{
    vast::predicate{vast::field_extractor{".x"}, vast::equal, vast::data{0u}}};
  auto x_equals_one = vast::expression{
    vast::predicate{vast::field_extractor{".x"}, vast::equal, vast::data{1u}}};
  auto type_equals_y = vast::expression{
    vast::predicate{vast::attribute_extractor{vast::atom::type_v}, vast::equal,
                    vast::data{"y"}}};
  auto type_equals_foo = vast::expression{
    vast::predicate{vast::attribute_extractor{vast::atom::type_v}, vast::equal,
                    vast::data{"foo"}}};
  // // For the query `x == 0`, we expect one indexer (for field x) and one
  // result.
  test_expression(x_equals_zero, 1, 1);
  // // For the query `x == 1`, we expect one indexer (for field x) and zero
  // results.
  test_expression(x_equals_one, 1, 0);
  // // For the query `#type == "x"`, we expect one indexer (a one-shot indexer
  // for type queries) and one result.
  test_expression(type_equals_y, 1, 1);
  // // For the query `#type == "foo"`, we expect one indexer (a one-shot
  // indexer for type queries) and no results.
  test_expression(type_equals_foo, 1, 0);
  // Shut down test actors.
  self->send_exit(readonly_partition, caf::exit_reason::user_shutdown);
  self->send_exit(fs, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()
