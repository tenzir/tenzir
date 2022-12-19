//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE active_partition

#include "vast/system/active_partition.hpp"

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/detail/partition_common.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/span.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/taxonomies.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include <caf/error.hpp>
#include <flatbuffers/flatbuffers.h>

#include <filesystem>
#include <map>
#include <span>

namespace {

vast::system::filesystem_actor::behavior_type
dummy_filesystem(std::reference_wrapper<
                 std::map<std::filesystem::path, std::vector<vast::chunk_ptr>>>
                   last_written_chunks) {
  return {
    [last_written_chunks](
      vast::atom::write, const std::filesystem::path& path,
      const vast::chunk_ptr& chk) mutable -> caf::result<vast::atom::ok> {
      MESSAGE("Received write request for path: " + path.string());
      last_written_chunks.get()[path].push_back(chk);
      return vast::atom::ok_v;
    },
    [](vast::atom::read,
       const std::filesystem::path&) -> caf::result<vast::chunk_ptr> {
      return vast::chunk_ptr{};
    },
    [](vast::atom::mmap,
       const std::filesystem::path&) -> caf::result<vast::chunk_ptr> {
      return vast::chunk_ptr{};
    },
    [](vast::atom::erase,
       const std::filesystem::path&) -> caf::result<vast::atom::done> {
      return vast::atom::done_v;
    },
    [](vast::atom::status, vast::system::status_verbosity) {
      return vast::record{};
    },
    [](vast::atom::move,
       std::vector<std::pair<std::filesystem::path, std::filesystem::path>>) {
      return vast::atom::done_v;
    },
    [](vast::atom::move, const std::filesystem::path&,
       const std::filesystem::path&) -> caf::result<vast::atom::done> {
      return vast::atom::done_v;
    },
    [](vast::atom::telemetry) {
      // nop
    },
  };
}

vast::system::store_actor::behavior_type dummy_store(
  std::reference_wrapper<std::vector<vast::query_context>> last_query_contexts) {
  return {
    [last_query_contexts](vast::atom::query, const vast::query_context& ctx)
      -> caf::result<uint64_t> {
      last_query_contexts.get().push_back(ctx);
      return 0u;
    },
    [](const vast::atom::erase&, const vast::ids&) -> caf::result<uint64_t> {
      return 0u;
    },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
  }

  vast::type schema_{
    "y",
    vast::record_type{
      {"x", vast::count_type{}},
    },
  };

  vast::index_config index_config_{
    .rules = {{.targets = {"y.x"}, .create_partition_index = false}}};
};

FIXTURE_SCOPE(active_partition_tests, fixture)

TEST(No dense indexes serialization when create dense index in config is false) {
  std::map<std::filesystem::path, std::vector<vast::chunk_ptr>>
    last_written_chunks;
  auto filesystem = sys.spawn(dummy_filesystem, std::ref(last_written_chunks));
  const auto partition_id = vast::uuid::random();
  const std::string input_store_id = "some-id";
  auto header_data = 3523532ull;
  auto partition_header
    = vast::chunk::make(&header_data, sizeof(header_data), []() noexcept {});
  auto sut
    = sys.spawn(vast::system::active_partition, partition_id,
                vast::system::accountant_actor{}, filesystem, caf::settings{},
                index_config_, vast::system::store_actor{}, input_store_id,
                partition_header, std::make_shared<vast::taxonomies>());
  REQUIRE(sut);
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, schema_);
  CHECK(builder->add(0u));
  auto slice = builder->finish();
  slice.offset(0);
  const auto now = caf::make_timestamp();
  slice.import_time(now);
  auto src = vast::detail::spawn_container_source(sys, std::vector{slice}, sut);
  REQUIRE(src);
  run();
  auto persist_path = std::filesystem::path{"/persist"};
  auto synopsis_path = std::filesystem::path{"/synopsis"};
  auto promise = self->request(sut, caf::infinite, vast::atom::persist_v,
                               persist_path, synopsis_path);
  run();
  promise.receive([](vast::partition_synopsis_ptr&) {},
                  [](const caf::error& err) {
                    FAIL(err);
                  });
  // 1 chunk for partition synopsis and one for partition itself
  REQUIRE_EQUAL(last_written_chunks.size(), 2u);
  REQUIRE_EQUAL(last_written_chunks.at(persist_path).size(), 1u);
  REQUIRE_EQUAL(last_written_chunks.at(synopsis_path).size(), 1u);
  const auto& synopsis_chunk = last_written_chunks.at(synopsis_path).front();
  auto* synopsis_fbs = vast::fbs::GetPartitionSynopsis(synopsis_chunk->data());
  vast::partition_synopsis synopsis;
  CHECK(!unpack(*synopsis_fbs->partition_synopsis_as<
                  vast::fbs::partition_synopsis::LegacyPartitionSynopsis>(),
                synopsis));
  CHECK_EQUAL(synopsis.events, 1u);
  CHECK_EQUAL(synopsis.schema, schema_);
  CHECK_EQUAL(synopsis.min_import_time, now);
  CHECK_EQUAL(synopsis.max_import_time, now);
  CHECK_EQUAL(synopsis.field_synopses_.size(), 1u);
  CHECK_EQUAL(synopsis.type_synopses_.size(), 1u);
  CHECK_EQUAL(synopsis.offset, 0u);
  const auto& partition_chunk = last_written_chunks.at(persist_path).front();
  const auto container = vast::fbs::flatbuffer_container{partition_chunk};
  const auto part_fb
    = container.as_flatbuffer<vast::fbs::Partition>(0)
        ->partition_as<vast::fbs::partition::LegacyPartition>();
  vast::system::passive_partition_state passive_state;
  const auto err = unpack(*part_fb, passive_state);
  REQUIRE_EQUAL(err, caf::error{});
  CHECK_EQUAL(passive_state.id, partition_id);
  REQUIRE(passive_state.combined_layout_);
  CHECK_EQUAL(*passive_state.combined_layout_,
              (vast::record_type{{"y.x", vast::count_type{}}}));
  vast::ids expected_ids;
  expected_ids.append_bit(true);
  CHECK_EQUAL(passive_state.type_ids_.at(std::string{schema_.name()}),
              expected_ids);
  CHECK_EQUAL(passive_state.offset, 0u);
  CHECK_EQUAL(passive_state.events, 1u);
  CHECK_EQUAL(passive_state.store_id, input_store_id);
  CHECK_EQUAL(passive_state.store_header, as_bytes(partition_header));
  const auto* indexes = part_fb->indexes();
  REQUIRE_EQUAL(indexes->size(), 1u);
  CHECK_EQUAL(indexes->Get(0)->field_name()->str(), "y.x");
  CHECK_EQUAL(indexes->Get(0)->index()->caf_0_18_data(), nullptr);
}

TEST(Delegate query to store with all possible ids in partition when query is to
       be done without dense indexer) {
  std::vector<vast::query_context> last_query_contexts;
  auto store = sys.spawn(dummy_store, std::ref(last_query_contexts));

  auto sut = sys.spawn(vast::system::active_partition, vast::uuid::random(),
                       vast::system::accountant_actor{},
                       vast::system::filesystem_actor{}, caf::settings{},
                       index_config_, store, "some-id", vast::chunk_ptr{},
                       std::make_shared<vast::taxonomies>());

  REQUIRE(sut);

  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, schema_);

  CHECK(builder->add(0u));
  auto slice1 = builder->finish();
  slice1.offset(0);

  CHECK(builder->add(25u));
  auto slice2 = builder->finish();
  slice2.offset(1);

  auto src = vast::detail::spawn_container_source(
    sys, std::vector{slice1, slice2}, sut);
  REQUIRE(src);
  run();

  auto expr = vast::expression{vast::predicate{vast::field_extractor{"x"},
                                               vast::relational_operator::equal,
                                               vast::data{0u}}};
  auto query_context
    = vast::query_context::make_extract("test", self, std::move(expr));

  auto promise
    = self->request(sut, caf::infinite, vast::atom::query_v, query_context);
  run();
  promise.receive(
    [](uint64_t) {
      MESSAGE("query done");
    },
    [](const caf::error& err) {
      FAIL(err);
    });

  REQUIRE_EQUAL(last_query_contexts.size(), 1u);

  vast::ids expected_ids;
  expected_ids.append_bits(true, 2);
  CHECK_EQUAL(last_query_contexts.back().ids, expected_ids);
}

FIXTURE_SCOPE_END()

} // namespace
