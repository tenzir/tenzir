//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/expression.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/msgpack_table_slice_builder.hpp"
#include "vast/span.hpp"
#include "vast/system/index.hpp"
#include "vast/system/meta_index.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <flatbuffers/flatbuffers.h>

#define SUITE flatbuffers
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <cstddef>
#include <filesystem>

using vast::span;

TEST(uuid roundtrip) {
  vast::uuid uuid = vast::uuid::random();
  auto expected_fb = vast::fbs::wrap(uuid);
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  vast::uuid uuid2 = vast::uuid::random();
  CHECK_NOT_EQUAL(uuid, uuid2);
  span<const std::byte> span{reinterpret_cast<const std::byte*>(fb->data()), fb->size()};
  vast::fbs::unwrap<vast::fbs::uuid::v0>(span, uuid2);
  CHECK_EQUAL(uuid, uuid2);
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
  flatbuffers::FlatBufferBuilder builder;
  auto index = pack(builder, state);
  REQUIRE(index);
  vast::fbs::FinishIndexBuffer(builder, *index);
  auto fb = builder.GetBufferPointer();
  auto sz = builder.GetSize();
  auto span = vast::span(fb, sz);
  // Deserialize the index.
  auto idx = vast::fbs::GetIndex(span.data());
  CHECK_EQUAL(idx->index_type(), vast::fbs::index::Index::v0);
  auto idx_v0 = idx->index_as_v0();
  // Check Index state.
  auto partition_uuids = idx_v0->partitions();
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
  auto stats = idx_v0->stats();
  REQUIRE(stats);
  REQUIRE_EQUAL(stats->size(), 1u);
  REQUIRE(stats->Get(0));
  CHECK_EQUAL(stats->Get(0)->name()->str(), std::string{"zeek.conn"});
  CHECK_EQUAL(stats->Get(0)->count(), 54931u);
}

FIXTURE_SCOPE(partition_roundtrips, fixtures::deterministic_actor_system)

TEST(empty partition roundtrip) {
  // Init factory.
  vast::factory<vast::table_slice_builder>::initialize();
  // Create partition state.
  vast::system::active_partition_state state;
  state.id = vast::uuid::random();
  state.offset = 17;
  state.events = 23;
  state.synopsis = std::make_shared<vast::partition_synopsis>();
  state.combined_layout
    = vast::record_type{{"x", vast::count_type{}}}.name("y");
  auto& ids = state.type_ids["x"];
  ids.append_bits(0, 3);
  ids.append_bits(1, 3);
  // Prepare a layout for the partition synopsis. The partition synopsis only
  // looks at the layout of the table slices it gets, so we feed it
  // with an empty table slice.
  auto layout = vast::record_type{{"x", vast::count_type{}}}.name("y");
  auto slice_builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, layout);
  REQUIRE(slice_builder);
  auto slice = slice_builder->finish();
  slice.offset(0);
  REQUIRE_NOT_EQUAL(slice.encoding(), vast::table_slice_encoding::none);
  state.synopsis->add(slice, caf::settings{});
  // Serialize partition.
  flatbuffers::FlatBufferBuilder builder;
  {
    auto partition = pack(builder, state);
    REQUIRE(partition);
    vast::fbs::FinishPartitionBuffer(builder, *partition);
  }
  auto ptr = builder.GetBufferPointer();
  auto sz = builder.GetSize();
  vast::span span(ptr, sz);
  // Deserialize partition.
  vast::system::passive_partition_state recovered_state = {};
  auto partition = vast::fbs::GetPartition(span.data());
  REQUIRE(partition);
  REQUIRE_EQUAL(partition->partition_type(),
                vast::fbs::partition::Partition::v0);
  auto partition_v0 = partition->partition_as_v0();
  REQUIRE(partition_v0);
  auto error = unpack(*partition_v0, recovered_state);
  CHECK(!error);
  CHECK_EQUAL(recovered_state.id, state.id);
  CHECK_EQUAL(recovered_state.offset, state.offset);
  CHECK_EQUAL(recovered_state.events, state.events);
  CHECK_EQUAL(recovered_state.combined_layout, state.combined_layout);
  CHECK_EQUAL(recovered_state.type_ids, state.type_ids);
  // Deserialize meta index state from this partition.
  auto ps = std::make_shared<vast::partition_synopsis>();
  auto error2 = vast::system::unpack(*partition_v0, *ps);
  CHECK(!error2);
  CHECK_EQUAL(ps->field_synopses_.size(), 1u);
  auto meta_index = self->spawn(vast::system::meta_index);
  auto rp = self->request(meta_index, caf::infinite, vast::atom::merge_v,
                          recovered_state.id, ps);
  run();
  rp.receive([=](vast::atom::ok) {}, [=](const caf::error& err) { FAIL(err); });
  auto rp2
    = self->request(meta_index, caf::infinite,
                    vast::expression{vast::predicate{
                      vast::field_extractor{".x"},
                      vast::relational_operator::equal, vast::data{0u}}});
  run();
  rp2.receive(
    [&](const std::vector<vast::uuid>& candidates) {
      REQUIRE_EQUAL(candidates.size(), 1ull);
      CHECK_EQUAL(candidates[0], state.id);
    },
    [=](const caf::error& err) { FAIL(err); });
}

// This test spawns a partition, fills it with some test data, then persists
// the partition to disk, restores it from the persisted on-disk state, and
// finally does some queries on it to ensure the restored flatbuffer is still
// able to return correct results.
// TODO: Bring back.
//TEST(full partition roundtrip) {
//  // Spawn a partition.
//  auto fs = self->spawn(
//    vast::system::posix_filesystem,
//    directory); // `directory` is provided by the unit test fixture
//  auto partition_uuid = vast::uuid::random();
//  auto partition = sys.spawn(vast::system::active_partition, partition_uuid, fs,
//                             caf::settings{}, caf::settings{});
//  run();
//  REQUIRE(partition);
//  // Add data to the partition.
//  auto layout = vast::record_type{{"x", vast::count_type{}}}.name("y");
//  auto builder = vast::msgpack_table_slice_builder::make(layout);
//  CHECK(builder->add(0u));
//  auto slice = builder->finish();
//  slice.offset(0);
//  auto data = std::vector<vast::table_slice>{slice};
//  auto src = vast::detail::spawn_container_source(sys, data, partition);
//  REQUIRE(src);
//  run();
//  // Persist the partition to disk;
//  std::filesystem::path persist_path
//    = "test-partition"; // will be interpreted relative to
//                        // the fs actor's root dir
//  std::filesystem::path synopsis_path = "test-partition-synopsis";
//  auto persist_promise
//    = self->request(partition, caf::infinite, vast::atom::persist_v,
//                    persist_path, synopsis_path);
//  run();
//  persist_promise.receive(
//    [](std::shared_ptr<vast::partition_synopsis>&) {
//      CHECK("persisting done");
//    },
//    [](caf::error err) { FAIL(err); });
//  self->send_exit(partition, caf::exit_reason::user_shutdown);
//  // Spawn a read-only partition from this chunk and try to query the data we
//  // added. We make two queries, one "#type"-query and one "normal" query
//  auto readonly_partition = sys.spawn(vast::system::passive_partition,
//                                      partition_uuid, fs, persist_path);
//  REQUIRE(readonly_partition);
//  run();
//  // A minimal `partition_client_actor`that stores the results in a local
//  // variable.
//  auto dummy_client = [](std::shared_ptr<vast::ids> ids)
//    -> vast::system::partition_client_actor::behavior_type {
//    return {
//      [ids](const vast::ids& hits) { *ids |= hits; },
//    };
//  };
//  auto test_expression
//    = [&](const vast::expression& expression, size_t expected_ids) {
//        bool done;
//        auto results = std::make_shared<vast::ids>();
//        auto dummy = self->spawn(dummy_client, results);
//        auto rp
//          = self->request(readonly_partition, caf::infinite, expression, dummy);
//        run();
//        rp.receive([&done](vast::atom::done) { done = true; },
//                   [](caf::error) { REQUIRE(false); });
//        run();
//        self->send_exit(dummy, caf::exit_reason::user_shutdown);
//        run();
//        CHECK_EQUAL(done, true);
//        CHECK_EQUAL(rank(*results), expected_ids);
//        return true;
//      };
//  auto x_equals_zero = vast::expression{
//    vast::predicate{vast::field_extractor{"x"},
//                    vast::relational_operator::equal, vast::data{0u}}};
//  auto x_equals_one = vast::expression{
//    vast::predicate{vast::field_extractor{"x"},
//                    vast::relational_operator::equal, vast::data{1u}}};
//  auto type_equals_y = vast::expression{
//    vast::predicate{vast::meta_extractor{vast::meta_extractor::type},
//                    vast::relational_operator::equal, vast::data{"y"}}};
//  auto type_equals_foo = vast::expression{
//    vast::predicate{vast::meta_extractor{vast::meta_extractor::type},
//                    vast::relational_operator::equal, vast::data{"foo"}}};
//  // For the query `x == 0`, we expect one result.
//  test_expression(x_equals_zero, 1);
//  // For the query `x == 1`, we expect zero results.
//  test_expression(x_equals_one, 0);
//  // For the query `#type == "x"`, we expect one result.
//  test_expression(type_equals_y, 1);
//  // For the query `#type == "foo"`, we expect no results.
//  test_expression(type_equals_foo, 0);
//  // Shut down test actors.
//  self->send_exit(readonly_partition, caf::exit_reason::user_shutdown);
//  self->send_exit(fs, caf::exit_reason::user_shutdown);
//  run();
//}

FIXTURE_SCOPE_END()
