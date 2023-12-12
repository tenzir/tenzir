//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/active_partition.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/aliases.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/detail/partition_common.hpp"
#include "tenzir/detail/spawn_container_source.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/span.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/test/fixtures/actor_system_and_events.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"
#include "tenzir/view.hpp"

#include <caf/error.hpp>
#include <flatbuffers/flatbuffers.h>

#include <filesystem>
#include <map>
#include <span>

namespace {

tenzir::filesystem_actor::behavior_type
dummy_filesystem(std::reference_wrapper<
                 std::map<std::filesystem::path, std::vector<tenzir::chunk_ptr>>>
                   last_written_chunks) {
  return {
    [last_written_chunks](
      tenzir::atom::write, const std::filesystem::path& path,
      const tenzir::chunk_ptr& chk) mutable -> caf::result<tenzir::atom::ok> {
      MESSAGE("Received write request for path: " + path.string());
      last_written_chunks.get()[path].push_back(chk);
      return tenzir::atom::ok_v;
    },
    [](tenzir::atom::read,
       const std::filesystem::path&) -> caf::result<tenzir::chunk_ptr> {
      return tenzir::chunk_ptr{};
    },
    [](tenzir::atom::mmap,
       const std::filesystem::path&) -> caf::result<tenzir::chunk_ptr> {
      return tenzir::chunk_ptr{};
    },
    [](tenzir::atom::erase,
       const std::filesystem::path&) -> caf::result<tenzir::atom::done> {
      return tenzir::atom::done_v;
    },
    [](tenzir::atom::status, tenzir::status_verbosity, tenzir::duration) {
      return tenzir::record{};
    },
    [](tenzir::atom::move,
       std::vector<std::pair<std::filesystem::path, std::filesystem::path>>) {
      return tenzir::atom::done_v;
    },
    [](tenzir::atom::move, const std::filesystem::path&,
       const std::filesystem::path&) -> caf::result<tenzir::atom::done> {
      return tenzir::atom::done_v;
    },
  };
}

tenzir::store_builder_actor::behavior_type
dummy_store(std::reference_wrapper<std::vector<tenzir::query_context>>
              last_query_contexts) {
  return {
    [last_query_contexts](tenzir::atom::query, const tenzir::query_context& ctx)
      -> caf::result<uint64_t> {
      last_query_contexts.get().push_back(ctx);
      return 0u;
    },
    [](const tenzir::atom::erase&,
       const tenzir::ids&) -> caf::result<uint64_t> {
      return 0u;
    },
    [](const tenzir::atom::persist&) -> caf::result<tenzir::resource> {
      return {};
    },
    [](caf::stream<tenzir::table_slice>)
      -> caf::result<caf::inbound_stream_slot<tenzir::table_slice>> {
      return tenzir::ec::no_error;
    },
    [](tenzir::atom::status,
       [[maybe_unused]] tenzir::status_verbosity verbosity, tenzir::duration) {
      return tenzir::record{
        {"foo", "bar"},
      };
    },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      TENZIR_PP_STRINGIFY(SUITE)) {
  }

  tenzir::type schema_{
    "y",
    tenzir::record_type{
      {"x", tenzir::uint64_type{}},
      {"z", tenzir::double_type{}},
    },
  };

  tenzir::index_config index_config_{
    .rules = {{.targets = {"y.x"}, .create_partition_index = false}}};
};

FIXTURE_SCOPE(active_partition_tests, fixture)

TEST(No dense indexes serialization when create dense index in config is false) {
  std::map<std::filesystem::path, std::vector<tenzir::chunk_ptr>>
    last_written_chunks;
  auto filesystem = sys.spawn(dummy_filesystem, std::ref(last_written_chunks));
  const auto partition_id = tenzir::uuid::random();
  // TODO: We should implement a mock store and use that for this test.
  const auto* store_plugin = tenzir::plugins::find<tenzir::store_actor_plugin>(
    tenzir::defaults::store_backend);
  REQUIRE(store_plugin);
  auto sut = sys.spawn(tenzir::active_partition, schema_, partition_id,
                       tenzir::accountant_actor{}, filesystem, caf::settings{},
                       index_config_, store_plugin,
                       std::make_shared<tenzir::taxonomies>());
  REQUIRE(sut);
  auto builder = std::make_shared<tenzir::table_slice_builder>(schema_);
  CHECK(builder->add(0u));
  CHECK(builder->add(0.1));
  auto slice = builder->finish();
  slice.offset(0);
  const auto now = caf::make_timestamp();
  slice.import_time(now);
  auto src
    = tenzir::detail::spawn_container_source(sys, std::vector{slice}, sut);
  REQUIRE(src);
  run();
  auto persist_path = std::filesystem::path{"/persist"};
  auto synopsis_path = std::filesystem::path{"/synopsis"};
  auto promise = self->request(sut, caf::infinite, tenzir::atom::persist_v,
                               persist_path, synopsis_path);
  run();
  promise.receive([](tenzir::partition_synopsis_ptr&) {},
                  [](const caf::error& err) {
                    FAIL(err);
                  });
  // Three chunks: partition, partition_synopsis, and the store.
  // This depends on which store is used, but we use the default feather
  // implementation here so the assumption of one file is ok.
  REQUIRE_EQUAL(last_written_chunks.size(), 3u);
  REQUIRE_EQUAL(last_written_chunks.at(persist_path).size(), 1u);
  REQUIRE_EQUAL(last_written_chunks.at(synopsis_path).size(), 1u);
  const auto& synopsis_chunk = last_written_chunks.at(synopsis_path).front();
  const auto* synopsis_fbs
    = tenzir::fbs::GetPartitionSynopsis(synopsis_chunk->data());
  tenzir::partition_synopsis synopsis;
  CHECK(!unpack(*synopsis_fbs->partition_synopsis_as<
                  tenzir::fbs::partition_synopsis::LegacyPartitionSynopsis>(),
                synopsis));
  CHECK_EQUAL(synopsis.events, 1u);
  CHECK_EQUAL(synopsis.schema, schema_);
  CHECK_EQUAL(synopsis.min_import_time, now);
  CHECK_EQUAL(synopsis.max_import_time, now);
  CHECK_EQUAL(synopsis.field_synopses_.size(), 2u);
  CHECK_EQUAL(synopsis.type_synopses_.size(), 2u);
  const auto& partition_chunk = last_written_chunks.at(persist_path).front();
  const auto container = tenzir::fbs::flatbuffer_container{partition_chunk};
  const auto* part_fb
    = container.as_flatbuffer<tenzir::fbs::Partition>(0)
        ->partition_as<tenzir::fbs::partition::LegacyPartition>();
  tenzir::passive_partition_state passive_state;
  const auto err = unpack(*part_fb, passive_state);
  REQUIRE_EQUAL(err, caf::error{});
  CHECK_EQUAL(passive_state.id, partition_id);
  REQUIRE(passive_state.combined_schema_);
  CHECK_EQUAL(*passive_state.combined_schema_,
              (tenzir::record_type{{"y.x", tenzir::uint64_type{}},
                                   {"y.z", tenzir::double_type{}}}));
  tenzir::ids expected_ids;
  expected_ids.append_bit(true);
  CHECK_EQUAL(passive_state.type_ids_.at(std::string{schema_.name()}),
              expected_ids);
  CHECK_EQUAL(passive_state.events, 1u);
  const auto* indexes = part_fb->indexes();
  REQUIRE_EQUAL(indexes->size(), 2u);
  CHECK_EQUAL(indexes->Get(0)->field_name()->str(), "y.x");
  CHECK_EQUAL(indexes->Get(0)->index()->caf_0_18_data(), nullptr);
  MESSAGE("check value index correctness");
  CHECK_EQUAL(indexes->Get(1)->field_name()->str(), "y.z");
  CHECK_EQUAL(indexes->Get(1)->index()->caf_0_18_data(), nullptr);
}

TEST(delegate query to the store) {
  // FIXME: We should implement a mock store plugin and use that for this test.
  const auto* store_plugin = tenzir::plugins::find<tenzir::store_actor_plugin>(
    tenzir::defaults::store_backend);
  std::map<std::filesystem::path, std::vector<tenzir::chunk_ptr>>
    last_written_chunks;
  auto filesystem = sys.spawn(dummy_filesystem, std::ref(last_written_chunks));
  auto sut = sys.spawn(tenzir::active_partition, schema_,
                       tenzir::uuid::random(), tenzir::accountant_actor{},
                       filesystem, caf::settings{}, index_config_, store_plugin,
                       std::make_shared<tenzir::taxonomies>());
  REQUIRE(sut);
  run();
  auto& state = deref<tenzir::active_partition_actor::stateful_impl<
    tenzir::active_partition_state>>(sut)
                  .state;
  std::vector<tenzir::query_context> last_query_contexts;
  state.store_builder = sys.spawn(dummy_store, std::ref(last_query_contexts));
  auto builder = std::make_shared<tenzir::table_slice_builder>(schema_);
  CHECK(builder->add(0u));
  CHECK(builder->add(0.1));
  auto slice1 = builder->finish();
  slice1.offset(0);
  CHECK(builder->add(25u));
  CHECK(builder->add(3.1415));
  auto slice2 = builder->finish();
  slice2.offset(1);
  auto src = tenzir::detail::spawn_container_source(
    sys, std::vector{slice1, slice2}, sut);
  REQUIRE(src);
  run();
  auto expr = tenzir::expression{
    tenzir::predicate{tenzir::field_extractor{"x"},
                      tenzir::relational_operator::equal, tenzir::data{0u}}};
  auto query_context
    = tenzir::query_context::make_extract("test", self, std::move(expr));
  auto promise
    = self->request(sut, caf::infinite, tenzir::atom::query_v, query_context);
  run();
  promise.receive(
    [](uint64_t) {
      MESSAGE("query done");
    },
    [](const caf::error& err) {
      FAIL(err);
    });
  REQUIRE_EQUAL(last_query_contexts.size(), 1u);
}

FIXTURE_SCOPE_END()

} // namespace
