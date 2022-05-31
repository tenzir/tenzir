//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition_roundtrip

#include "vast/fwd.hpp"

#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/query.hpp"
#include "vast/system/active_partition.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/index.hpp"
#include "vast/system/passive_partition.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <filesystem>
#include <span>

vast::system::store_actor::behavior_type dummy_store() {
  return {[](const vast::query&) {
            return uint64_t{0};
          },
          [](const vast::atom::erase&, const vast::ids&) {
            return uint64_t{0};
          }};
}

using std::span;

TEST(uuid roundtrip) {
  vast::uuid uuid = vast::uuid::random();
  auto expected_fb = vast::fbs::wrap(uuid);
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  vast::uuid uuid2 = vast::uuid::random();
  CHECK_NOT_EQUAL(uuid, uuid2);
  std::span<const std::byte> span{
    reinterpret_cast<const std::byte*>(fb->data()), fb->size()};
  auto error = vast::fbs::unwrap<vast::fbs::LegacyUUID>(span, uuid2);
  CHECK(!error);
  CHECK_EQUAL(uuid, uuid2);
}

TEST(index roundtrip) {
  vast::system::index_state state(/*self = */ nullptr);
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
  state.stats.layouts["zeek.conn"] = vast::layout_statistics{54931u};
  // Serialize the index.
  flatbuffers::FlatBufferBuilder builder;
  auto index = pack(builder, state);
  REQUIRE(index);
  vast::fbs::FinishIndexBuffer(builder, *index);
  auto fb = builder.GetBufferPointer();
  auto sz = builder.GetSize();
  auto span = std::span(fb, sz);
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
    auto error = vast::unpack(*uuid, restored_uuid);
    CHECK(!error);
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

namespace {

struct fixture : fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(partition_roundtrips, fixture)

TEST(empty partition roundtrip) {
  // Init factory.
  vast::factory<vast::table_slice_builder>::initialize();
  // Create partition state.
  vast::system::active_partition_state state;
  state.data.id = vast::uuid::random();
  state.data.store_id = "legacy_archive";
  state.data.store_header = vast::chunk::make_empty();
  state.data.offset = 17;
  state.data.events = 23;
  state.data.synopsis = caf::make_copy_on_write<vast::partition_synopsis>();
  state.data.synopsis.unshared().offset = state.data.offset;
  state.data.synopsis.unshared().events = state.data.events;
  auto& ids = state.data.type_ids["x"];
  ids.append_bits(false, 3);
  ids.append_bits(true, 3);
  // Prepare a layout for the partition synopsis. The partition synopsis only
  // looks at the layout of the table slices it gets, so we feed it
  // with an empty table slice.
  auto layout = vast::type{
    "y",
    vast::record_type{
      {"x", vast::count_type{}},
    },
  };
  auto qf = vast::qualified_record_field{layout, vast::offset{0}};
  state.indexers[qf] = nullptr;
  auto slice_builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, layout);
  REQUIRE(slice_builder);
  auto slice = slice_builder->finish();
  slice.offset(0);
  REQUIRE_NOT_EQUAL(slice.encoding(), vast::table_slice_encoding::none);
  state.data.synopsis.unshared().add(
    slice, vast::defaults::system::max_partition_size, vast::index_config{});
  // Serialize partition.
  flatbuffers::FlatBufferBuilder builder;
  {
    auto combined_layout = state.combined_layout();
    REQUIRE(combined_layout);
    auto partition = pack(builder, state.data, *combined_layout);
    REQUIRE(partition);
    vast::fbs::FinishPartitionBuffer(builder, *partition);
  }
  auto ptr = builder.GetBufferPointer();
  auto sz = builder.GetSize();
  std::span span(ptr, sz);
  // Deserialize partition.
  vast::system::passive_partition_state recovered_state = {};
  auto partition = vast::fbs::GetPartition(span.data());
  REQUIRE(partition);
  REQUIRE_EQUAL(partition->partition_type(),
                vast::fbs::partition::Partition::legacy);
  auto partition_legacy = partition->partition_as_legacy();
  REQUIRE(partition_legacy);
  REQUIRE(partition_legacy->store());
  REQUIRE(partition_legacy->store()->id());
  CHECK_EQUAL(partition_legacy->store()->id()->str(), "legacy_archive");
  CHECK_EQUAL(partition_legacy->offset(), state.data.offset);
  CHECK_EQUAL(partition_legacy->events(), state.data.events);
  auto error = unpack(*partition_legacy, recovered_state);
  CHECK(!error);
  CHECK_EQUAL(recovered_state.id, state.data.id);
  CHECK_EQUAL(recovered_state.offset, state.data.offset);
  CHECK_EQUAL(recovered_state.events, state.data.events);
  // As of the Type FlatBuffers change we no longer keep the combined layout in
  // the active partition, which makes this test irrelevant:
  //   CHECK_EQUAL(recovered_state.combined_layout_, state.combined_layout);
  CHECK_EQUAL(recovered_state.type_ids_, state.data.type_ids);
  // Deserialize catalog state from this partition.
  auto ps = caf::make_copy_on_write<vast::partition_synopsis>();
  auto error2 = vast::system::unpack(*partition_legacy, ps.unshared());
  CHECK(!error2);
  CHECK_EQUAL(ps->field_synopses_.size(), 1u);
  CHECK_EQUAL(ps->offset, state.data.offset);
  CHECK_EQUAL(ps->events, state.data.events);
  auto catalog
    = self->spawn(vast::system::catalog, vast::system::accountant_actor{});
  auto rp = self->request(catalog, caf::infinite, vast::atom::merge_v,
                          recovered_state.id, ps);
  run();
  rp.receive([=](vast::atom::ok) {},
             [=](const caf::error& err) {
               FAIL(err);
             });
  auto expr = vast::expression{vast::predicate{
    vast::extractor{"x"}, vast::relational_operator::equal, vast::data{0u}}};
  auto q = vast::query::make_extract(self, std::move(expr));
  auto rp2 = self->request(catalog, caf::infinite, vast::atom::candidates_v,
                           std::move(q));
  run();
  rp2.receive(
    [&](const vast::system::catalog_result& result) {
      const auto& candidates = result.partitions;
      REQUIRE_EQUAL(candidates.size(), 1ull);
      CHECK_EQUAL(candidates[0], state.data.id);
    },
    [=](const caf::error& err) {
      FAIL(err);
    });
}

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
  auto store_id = std::string{"legacy_archive"};
  auto partition
    = sys.spawn(vast::system::active_partition, partition_uuid,
                vast::system::accountant_actor{}, fs, caf::settings{},
                vast::index_config{}, vast::system::store_actor{}, store_id,
                vast::chunk::make_empty());
  run();
  REQUIRE(partition);
  // Add data to the partition.
  auto layout = vast::type{
    "y",
    vast::record_type{
      {"x", vast::count_type{}},
    },
  };
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, layout);
  CHECK(builder->add(0u));
  auto slice = builder->finish();
  slice.offset(0);
  auto data = std::vector<vast::table_slice>{slice};
  auto src = vast::detail::spawn_container_source(sys, data, partition);
  REQUIRE(src);
  run();
  // Persist the partition to disk;
  std::filesystem::path persist_path
    = "test-partition"; // will be interpreted relative to
                        // the fs actor's root dir
  std::filesystem::path synopsis_path = "test-partition-synopsis";
  auto persist_promise
    = self->request(partition, caf::infinite, vast::atom::persist_v,
                    persist_path, synopsis_path);
  run();
  persist_promise.receive(
    [](vast::partition_synopsis_ptr&) {
      CHECK("persisting done");
    },
    [](const caf::error& err) {
      FAIL(err);
    });
  self->send_exit(partition, caf::exit_reason::user_shutdown);
  // Spawn a read-only partition from this chunk and try to query the data we
  // added. We make two queries, one "#type"-query and one "normal" query
  auto archive = sys.spawn(dummy_store);
  // auto archive =
  // vast::system::store_actor::behavior_type::make_empty_behavior();
  auto readonly_partition
    = sys.spawn(vast::system::passive_partition, partition_uuid,
                vast::system::accountant_actor{}, archive, fs, persist_path);
  REQUIRE(readonly_partition);
  run();
  // A minimal `partition_client_actor`that stores the results in a local
  // variable.
  auto dummy_client = [](std::shared_ptr<uint64_t> count)
    -> vast::system::receiver_actor<uint64_t>::behavior_type {
    return {
      [count](uint64_t hits) {
        *count += hits;
      },
    };
  };
  auto test_expression
    = [&](const vast::expression& expression, size_t expected_hits) {
        uint64_t tally = 0;
        auto result = std::make_shared<uint64_t>();
        auto dummy = self->spawn(dummy_client, result);
        auto rp = self->request(
          readonly_partition, caf::infinite,
          vast::query::make_count(dummy, vast::query::count::mode::estimate,
                                  expression));
        run();
        rp.receive(
          [&tally](uint64_t x) {
            tally = x;
          },
          [](caf::error&) {
            REQUIRE(false);
          });
        run();
        self->send_exit(dummy, caf::exit_reason::user_shutdown);
        run();
        CHECK_EQUAL(*result, expected_hits);
        CHECK_EQUAL(tally, expected_hits);
        return true;
      };
  auto x_equals_zero = vast::expression{vast::predicate{
    vast::extractor{"x"}, vast::relational_operator::equal, vast::data{0u}}};
  auto x_equals_one = vast::expression{vast::predicate{
    vast::extractor{"x"}, vast::relational_operator::equal, vast::data{1u}}};
  auto foo_equals_one = vast::expression{vast::predicate{
    vast::extractor{"foo"}, vast::relational_operator::equal, vast::data{1u}}};
  auto type_equals_y = vast::expression{
    vast::predicate{vast::selector{vast::selector::type},
                    vast::relational_operator::equal, vast::data{"y"}}};
  auto type_equals_foo = vast::expression{
    vast::predicate{vast::selector{vast::selector::type},
                    vast::relational_operator::equal, vast::data{"foo"}}};
  // For the query `x == 0`, we expect one result.
  test_expression(x_equals_zero, 1);
  // For the query `x == 1`, we expect zero results.
  test_expression(x_equals_one, 0);
  // For the query `foo == 1`, we expect zero results.
  test_expression(foo_equals_one, 0);
  // For the query `#type == "x"`, we expect one result.
  test_expression(type_equals_y, 1);
  // For the query `#type == "foo"`, we expect no results.
  test_expression(type_equals_foo, 0);
  // Shut down test actors.
  self->send_exit(readonly_partition, caf::exit_reason::user_shutdown);
  self->send_exit(fs, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()
