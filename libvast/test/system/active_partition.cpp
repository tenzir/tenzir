//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/active_partition.hpp"

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/chunk.hpp"
#include "vast/detail/partition_common.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/uuid.hpp"
#include "vast/span.hpp"
#include "vast/system/actors.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/taxonomies.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

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
    [](vast::atom::status, vast::system::status_verbosity, vast::duration) {
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
  };
}

vast::system::store_builder_actor::behavior_type dummy_store(
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
    [](caf::stream<vast::table_slice>)
      -> caf::result<caf::inbound_stream_slot<vast::table_slice>> {
      return vast::ec::no_error;
    },
    [](vast::atom::status,
       [[maybe_unused]] vast::system::status_verbosity verbosity,
       vast::duration) {
      return vast::record{
        {"foo", "bar"},
      };
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
      {"x", vast::uint64_type{}},
      {"z", vast::double_type{}},
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
  // TODO: We should implement a mock store and use that for this test.
  const auto* store_plugin = vast::plugins::find<vast::store_actor_plugin>(
    vast::defaults::system::store_backend);
  REQUIRE(store_plugin);
  auto sut = sys.spawn(vast::system::active_partition, schema_, partition_id,
                       vast::system::accountant_actor{}, filesystem,
                       caf::settings{}, index_config_, store_plugin,
                       std::make_shared<vast::taxonomies>());
  REQUIRE(sut);
  auto builder = std::make_shared<vast::table_slice_builder>(schema_);
  CHECK(builder->add(0u));
  CHECK(builder->add(0.1));
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
  // Three chunks: partition, partition_synopsis, and the store.
  // This depends on which store is used, but we use the default feather
  // implementation here so the assumption of one file is ok.
  REQUIRE_EQUAL(last_written_chunks.size(), 3u);
  REQUIRE_EQUAL(last_written_chunks.at(persist_path).size(), 1u);
  REQUIRE_EQUAL(last_written_chunks.at(synopsis_path).size(), 1u);
  const auto& synopsis_chunk = last_written_chunks.at(synopsis_path).front();
  const auto* synopsis_fbs
    = vast::fbs::GetPartitionSynopsis(synopsis_chunk->data());
  vast::partition_synopsis synopsis;
  CHECK(!unpack(*synopsis_fbs->partition_synopsis_as<
                  vast::fbs::partition_synopsis::LegacyPartitionSynopsis>(),
                synopsis));
  CHECK_EQUAL(synopsis.events, 1u);
  CHECK_EQUAL(synopsis.schema, schema_);
  CHECK_EQUAL(synopsis.min_import_time, now);
  CHECK_EQUAL(synopsis.max_import_time, now);
  CHECK_EQUAL(synopsis.field_synopses_.size(), 2u);
  CHECK_EQUAL(synopsis.type_synopses_.size(), 2u);
  const auto& partition_chunk = last_written_chunks.at(persist_path).front();
  const auto container = vast::fbs::flatbuffer_container{partition_chunk};
  const auto* part_fb
    = container.as_flatbuffer<vast::fbs::Partition>(0)
        ->partition_as<vast::fbs::partition::LegacyPartition>();
  vast::system::passive_partition_state passive_state;
  const auto err = unpack(*part_fb, passive_state);
  REQUIRE_EQUAL(err, caf::error{});
  CHECK_EQUAL(passive_state.id, partition_id);
  REQUIRE(passive_state.combined_schema_);
  CHECK_EQUAL(*passive_state.combined_schema_,
              (vast::record_type{{"y.x", vast::uint64_type{}},
                                 {"y.z", vast::double_type{}}}));
  vast::ids expected_ids;
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
  CHECK_NOT_EQUAL(indexes->Get(1)->index()->caf_0_18_data(), nullptr);
  auto col2_idx
    = vast::system::unpack_value_index(*indexes->Get(1)->index(), container);
  REQUIRE(col2_idx);
  CHECK_EQUAL(vast::double_type{}, col2_idx->type());
  auto result = col2_idx->lookup(vast::relational_operator::less,
                                 vast::make_data_view(1.0));
  CHECK_EQUAL(unbox(result), make_ids({0, 0}));
}

TEST(delegate query to the store) {
  // FIXME: We should implement a mock store plugin and use that for this test.
  const auto* store_plugin = vast::plugins::find<vast::store_actor_plugin>(
    vast::defaults::system::store_backend);
  std::map<std::filesystem::path, std::vector<vast::chunk_ptr>>
    last_written_chunks;
  auto filesystem = sys.spawn(dummy_filesystem, std::ref(last_written_chunks));
  auto sut = sys.spawn(vast::system::active_partition, schema_,
                       vast::uuid::random(), vast::system::accountant_actor{},
                       filesystem, caf::settings{}, index_config_, store_plugin,
                       std::make_shared<vast::taxonomies>());
  REQUIRE(sut);
  run();
  auto& state = deref<vast::system::active_partition_actor::stateful_impl<
    vast::system::active_partition_state>>(sut)
                  .state;
  std::vector<vast::query_context> last_query_contexts;
  state.store_builder = sys.spawn(dummy_store, std::ref(last_query_contexts));
  auto builder = std::make_shared<vast::table_slice_builder>(schema_);
  CHECK(builder->add(0u));
  CHECK(builder->add(0.1));
  auto slice1 = builder->finish();
  slice1.offset(0);
  CHECK(builder->add(25u));
  CHECK(builder->add(3.1415));
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
}

FIXTURE_SCOPE_END()

} // namespace
