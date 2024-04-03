//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/actors.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/spawn_container_source.hpp"
#include "tenzir/fbs/index.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/index.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/test/fixtures/actor_system_and_events.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"

#include <caf/make_copy_on_write.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <filesystem>
#include <span>

tenzir::store_actor::behavior_type dummy_store() {
  return {
    [](tenzir::atom::query, const tenzir::query_context&) {
      return uint64_t{0};
    },
    [](const tenzir::atom::erase&, const tenzir::ids&) {
      return uint64_t{0};
    },
  };
}

using std::span;

TEST(uuid roundtrip) {
  tenzir::uuid uuid = tenzir::uuid::random();
  auto expected_fb = tenzir::fbs::wrap(uuid);
  REQUIRE(expected_fb);
  auto fb = *expected_fb;
  tenzir::uuid uuid2 = tenzir::uuid::random();
  CHECK_NOT_EQUAL(uuid, uuid2);
  std::span<const std::byte> span{
    reinterpret_cast<const std::byte*>(fb->data()), fb->size()};
  auto error = tenzir::fbs::unwrap<tenzir::fbs::LegacyUUID>(span, uuid2);
  CHECK(!error);
  CHECK_EQUAL(uuid, uuid2);
}

TEST(index roundtrip) {
  tenzir::index_state state(/*self = */ nullptr);
  // Both unpersisted and persisted partitions should show up in the created
  // flatbuffer.
  state.unpersisted[tenzir::uuid::random()] = {};
  state.unpersisted[tenzir::uuid::random()] = {};
  state.persisted_partitions.emplace(tenzir::uuid::random());
  state.persisted_partitions.emplace(tenzir::uuid::random());
  std::set<tenzir::uuid> expected_uuids;
  for (auto& kv : state.unpersisted)
    expected_uuids.insert(kv.first);
  for (auto& persisted : state.persisted_partitions)
    expected_uuids.insert(persisted);
  // Serialize the index.
  flatbuffers::FlatBufferBuilder builder;
  auto index = pack(builder, state);
  REQUIRE(index);
  auto fb = builder.GetBufferPointer();
  auto sz = builder.GetSize();
  auto span = std::span(fb, sz);
  // Deserialize the index.
  auto idx = tenzir::fbs::GetIndex(span.data());
  CHECK_EQUAL(idx->index_type(), tenzir::fbs::index::Index::v0);
  auto idx_v0 = idx->index_as_v0();
  // Check Index state.
  auto partition_uuids = idx_v0->partitions();
  REQUIRE(partition_uuids);
  CHECK_EQUAL(partition_uuids->size(), expected_uuids.size());
  std::set<tenzir::uuid> restored_uuids;
  for (auto uuid : *partition_uuids) {
    REQUIRE(uuid);
    tenzir::uuid restored_uuid;
    auto error = tenzir::unpack(*uuid, restored_uuid);
    CHECK(!error);
    restored_uuids.insert(restored_uuid);
  }
  CHECK_EQUAL(expected_uuids, restored_uuids);
}

namespace {

struct fixture : fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(TENZIR_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(partition_roundtrips, fixture)

TEST(empty partition roundtrip) {
  // Create partition state.
  tenzir::active_partition_state state;
  state.data.id = tenzir::uuid::random();
  state.data.store_id = tenzir::defaults::store_backend;
  state.data.store_header = tenzir::chunk::make_empty();
  state.data.events = 23;
  state.data.synopsis = caf::make_copy_on_write<tenzir::partition_synopsis>();
  state.data.synopsis.unshared().events = state.data.events;
  auto& ids = state.data.type_ids["x"];
  ids.append_bits(false, 3);
  ids.append_bits(true, 3);
  // Prepare a schema for the partition synopsis. The partition synopsis only
  // looks at the schema of the table slices it gets, so we feed it
  // with an empty table slice.
  auto schema = tenzir::type{
    "y",
    tenzir::record_type{
      {"x", tenzir::uint64_type{}},
    },
  };
  auto qf = tenzir::qualified_record_field{schema, tenzir::offset{0}};
  state.indexers[qf] = nullptr;
  auto slice_builder = std::make_shared<tenzir::table_slice_builder>(schema);
  REQUIRE(slice_builder);
  auto slice = slice_builder->finish();
  slice.offset(0);
  state.data.synopsis.unshared().add(
    slice, tenzir::defaults::max_partition_size, tenzir::index_config{});
  // Serialize partition.
  tenzir::chunk_ptr partition_chunk = {};
  {
    auto combined_schema = state.combined_schema();
    REQUIRE(combined_schema);
    auto partition = pack_full(state.data, *combined_schema);
    REQUIRE(partition);
    partition_chunk = *partition;
  }
  // Deserialize partition.
  tenzir::passive_partition_state recovered_state = {};
  auto container = tenzir::fbs::flatbuffer_container{partition_chunk};
  auto partition = container.as_flatbuffer<tenzir::fbs::Partition>(0);
  REQUIRE(partition);
  REQUIRE_EQUAL(partition->partition_type(),
                tenzir::fbs::partition::Partition::legacy);
  auto partition_legacy = partition->partition_as_legacy();
  REQUIRE(partition_legacy);
  REQUIRE(partition_legacy->store());
  REQUIRE(partition_legacy->store()->id());
  CHECK_EQUAL(partition_legacy->store()->id()->str(),
              tenzir::defaults::store_backend);
  CHECK_EQUAL(partition_legacy->events(), state.data.events);
  auto error = unpack(*partition_legacy, recovered_state);
  CHECK(!error);
  CHECK_EQUAL(recovered_state.id, state.data.id);
  CHECK_EQUAL(recovered_state.events, state.data.events);
  // As of the Type FlatBuffers change we no longer keep the combined schema in
  // the active partition, which makes this test irrelevant:
  //   CHECK_EQUAL(recovered_state.combined_schema_, state.combined_schema);
  CHECK_EQUAL(recovered_state.type_ids_, state.data.type_ids);
  // Deserialize catalog state from this partition.
  auto ps = caf::make_copy_on_write<tenzir::partition_synopsis>();
  auto error2 = tenzir::unpack(*partition_legacy, ps.unshared());
  CHECK(!error2);
  CHECK_EQUAL(ps->field_synopses_.size(), 1u);
  CHECK_EQUAL(ps->events, state.data.events);
  auto catalog = self->spawn(tenzir::catalog, tenzir::accountant_actor{},
                             directory / "types");
  auto rp = self->request(catalog, caf::infinite, tenzir::atom::merge_v,
                          recovered_state.id, ps);
  run();
  rp.receive([=](tenzir::atom::ok) {},
             [=](const caf::error& err) {
               FAIL(err);
             });
  auto expr = tenzir::expression{
    tenzir::predicate{tenzir::field_extractor{"x"},
                      tenzir::relational_operator::equal, tenzir::data{0u}}};
  auto query_context
    = tenzir::query_context::make_extract("test", self, std::move(expr));
  auto rp2 = self->request(catalog, caf::infinite, tenzir::atom::candidates_v,
                           std::move(query_context));
  run();
  rp2.receive(
    [&](const tenzir::catalog_lookup_result& candidates) {
      REQUIRE_EQUAL(candidates.candidate_infos.size(), 1ull);
      const auto& partition_infos
        = candidates.candidate_infos.begin()->second.partition_infos;
      REQUIRE_EQUAL(partition_infos.size(), 1ull);
      const auto& candidate_partition = partition_infos.front();
      CHECK_EQUAL(candidate_partition.uuid, state.data.id);
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
  auto schema = tenzir::type{
    "y",
    tenzir::record_type{
      {"x", tenzir::uint64_type{}},
    },
  };
  // Spawn a partition.
  auto fs = self->spawn(tenzir::posix_filesystem, directory,
                        tenzir::accountant_actor{});
  auto partition_uuid = tenzir::uuid::random();
  const auto* store_plugin = tenzir::plugins::find<tenzir::store_actor_plugin>(
    tenzir::defaults::store_backend);
  REQUIRE(store_plugin);
  auto partition = sys.spawn(tenzir::active_partition, schema, partition_uuid,
                             tenzir::accountant_actor{}, fs, caf::settings{},
                             tenzir::index_config{}, store_plugin,
                             std::make_shared<tenzir::taxonomies>());
  run();
  REQUIRE(partition);
  // Add data to the partition.
  auto builder = std::make_shared<tenzir::table_slice_builder>(schema);
  CHECK(builder->add(0u));
  auto slice = builder->finish();
  slice.offset(0);
  auto data = std::vector<tenzir::table_slice>{slice};
  auto src = tenzir::detail::spawn_container_source(sys, data, partition);
  REQUIRE(src);
  run();
  // Persist the partition to disk;
  std::filesystem::path persist_path
    = "test-partition"; // will be interpreted relative to
                        // the fs actor's root dir
  std::filesystem::path synopsis_path = "test-partition-synopsis";
  auto persist_promise
    = self->request(partition, caf::infinite, tenzir::atom::persist_v,
                    persist_path, synopsis_path);
  run();
  persist_promise.receive(
    [](tenzir::partition_synopsis_ptr&) {
      CHECK("persisting done");
    },
    [](const caf::error& err) {
      FAIL(err);
    });
  self->send_exit(partition, caf::exit_reason::user_shutdown);
  auto readonly_partition
    = sys.spawn(tenzir::passive_partition, partition_uuid,
                tenzir::accountant_actor{}, fs, persist_path);
  REQUIRE(readonly_partition);
  run();
  // A minimal `partition_client_actor`that stores the results in a local
  // variable.
  auto dummy_client = [](std::shared_ptr<uint64_t> count)
    -> tenzir::receiver_actor<uint64_t>::behavior_type {
    return {
      [count](uint64_t hits) {
        *count += hits;
      },
    };
  };
  auto test_expression = [&](const tenzir::expression& expression,
                             size_t expected_hits) {
    uint64_t tally = 0;
    auto result = std::make_shared<uint64_t>();
    auto dummy = self->spawn(dummy_client, result);
    auto rp = self->request(
      readonly_partition, caf::infinite, tenzir::atom::query_v,
      tenzir::query_context::make_count(
        "test", dummy, tenzir::count_query_context::mode::estimate,
        expression));
    run();
    rp.receive(
      [&tally](uint64_t x) {
        tally = x;
      },
      [](caf::error& e) {
        REQUIRE_EQUAL(e, caf::error{});
      });
    run();
    self->send_exit(dummy, caf::exit_reason::user_shutdown);
    run();
    MESSAGE("testing expression: " << expression);
    CHECK_EQUAL(*result, expected_hits);
    CHECK_EQUAL(tally, expected_hits);
    return true;
  };
  auto x_equals_zero = tenzir::expression{
    tenzir::predicate{tenzir::field_extractor{"x"},
                      tenzir::relational_operator::equal, tenzir::data{0u}}};
  auto x_equals_one = tenzir::expression{
    tenzir::predicate{tenzir::field_extractor{"x"},
                      tenzir::relational_operator::equal, tenzir::data{1u}}};
  auto foo_equals_one = tenzir::expression{
    tenzir::predicate{tenzir::field_extractor{"foo"},
                      tenzir::relational_operator::equal, tenzir::data{1u}}};
  auto type_equals_y = tenzir::expression{
    tenzir::predicate{tenzir::meta_extractor{tenzir::meta_extractor::schema},
                      tenzir::relational_operator::equal, tenzir::data{"y"}}};
  auto type_equals_foo = tenzir::expression{
    tenzir::predicate{tenzir::meta_extractor{tenzir::meta_extractor::schema},
                      tenzir::relational_operator::equal, tenzir::data{"foo"}}};
  // For the query `x == 0`, we expect one result.
  test_expression(x_equals_zero, 1);
  // For the query `x == 1`, we expect zero results.
  // This test is disabld as of Tenzir v4.3, for which this yields one result
  // because the dense indexes were disabled.
  // test_expression(x_equals_one, 0);
  // For the query `foo == 1`, we expect zero results.
  test_expression(foo_equals_one, 0);
  // For the query `#schema == "x"`, we expect one result.
  test_expression(type_equals_y, 1);
  // For the query `#schema == "foo"`, we expect no results.
  test_expression(type_equals_foo, 0);
  // Shut down test actors.
  self->send_exit(readonly_partition, caf::exit_reason::user_shutdown);
  self->send_exit(fs, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()
