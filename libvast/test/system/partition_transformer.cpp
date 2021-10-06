//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition_transformer

#include "vast/system/partition_transformer.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/index.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/memory_filesystem.hpp"
#include "vast/test/test.hpp"
#include "vast/transform.hpp"

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

using namespace std::string_literals;

namespace {

constexpr const auto IDSPACE_BEGIN = vast::id{42ull};

vast::system::idspace_distributor_actor::behavior_type mock_importer() {
  auto current_id = std::make_shared<vast::id>(IDSPACE_BEGIN);
  return {[=](vast::atom::reserve, uint64_t) {
    // Currently each test only reserves a single time; this actor will
    // need some state as soon as that changes.
    return IDSPACE_BEGIN;
  }};
}

vast::system::archive_actor::behavior_type mock_archive() {
  return vast::system::archive_actor::behavior_type::make_empty_behavior();
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    filesystem = self->spawn(memory_filesystem);
    importer = self->spawn(mock_importer);
  }

  fixture(const fixture&) = delete;
  fixture(fixture&&) = delete;
  fixture& operator=(const fixture&) = delete;
  fixture& operator=(fixture&&) = delete;

  ~fixture() override {
    self->send_exit(filesystem, caf::exit_reason::user_shutdown);
    self->send_exit(importer, caf::exit_reason::user_shutdown);
  }

  vast::system::idspace_distributor_actor importer;
  vast::system::filesystem_actor filesystem;
};

} // namespace

FIXTURE_SCOPE(partition_transformer_tests, fixture)

TEST(identity transform / done before persist) {
  // Spawn partition transformer
  auto uuid = vast::uuid::random();
  auto store_id = "segment-store"s;
  auto synopsis_opts = caf::settings{};
  auto index_opts = caf::settings{};
  auto transform = std::make_shared<vast::transform>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_step = vast::make_transform_step("identity", caf::settings{});
  REQUIRE_NOERROR(identity_step);
  transform->add_step(std::move(*identity_step));
  auto transformer = self->spawn(vast::system::partition_transformer, uuid,
                                 store_id, synopsis_opts, index_opts, importer,
                                 filesystem, std::move(transform));
  REQUIRE(transformer);
  // Stream data
  size_t events = 0;
  for (auto& slice : zeek_conn_log) {
    events += slice.rows();
    self->send(transformer, slice);
  }
  self->send(transformer, vast::atom::done_v);
  run();
  auto partition_path = std::filesystem::path{"/partition.fbs"};
  auto synopsis_path = std::filesystem::path{"/partition_synopsis.fbs"};
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v,
                          partition_path, synopsis_path);
  run();
  std::shared_ptr<vast::partition_synopsis> synopsis = nullptr;
  rp.receive(
    [&](std::shared_ptr<vast::partition_synopsis>& ps) {
      synopsis = ps;
    },
    [&](caf::error&) {
      FAIL("failed to persist");
    });
  // Verify serialized data
  auto partition_rp = self->request(filesystem, caf::infinite,
                                    vast::atom::read_v, partition_path);
  auto synopsis_rp = self->request(filesystem, caf::infinite,
                                   vast::atom::read_v, synopsis_path);
  run();
  partition_rp.receive(
    [&](vast::chunk_ptr& partition_chunk) {
      REQUIRE(partition_chunk);
      const auto* partition = vast::fbs::GetPartition(partition_chunk->data());
      REQUIRE_EQUAL(partition->partition_type(),
                    vast::fbs::partition::Partition::v0);
      const auto* partition_v0 = partition->partition_as_v0();
      CHECK_EQUAL(partition_v0->events(), events);
    },
    [](const caf::error&) {
      FAIL("failed to read stored partition");
    });
  synopsis_rp.receive(
    [&](vast::chunk_ptr& synopsis_chunk) {
      REQUIRE(synopsis_chunk);
      const auto* partition
        = vast::fbs::GetPartitionSynopsis(synopsis_chunk->data());
      REQUIRE_EQUAL(partition->partition_synopsis_type(),
                    vast::fbs::partition_synopsis::PartitionSynopsis::v0);
      const auto* synopsis_v0 = partition->partition_synopsis_as_v0();
      CHECK_EQUAL(synopsis_v0->id_range()->begin(), IDSPACE_BEGIN);
      CHECK_EQUAL(synopsis_v0->id_range()->end(), IDSPACE_BEGIN + events);
    },
    [](const caf::error&) {
      FAIL("failed to read stored synopsis");
    });
}

TEST(delete transform / persist before done) {
  // Spawn partition transformer
  auto uuid = vast::uuid::random();
  auto store_id = "segment-store"s;
  auto synopsis_opts = caf::settings{};
  auto index_opts = caf::settings{};
  auto plugin_opts = caf::settings{};
  plugin_opts["field"] = "uid";
  auto transform = std::make_shared<vast::transform>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_step = vast::make_transform_step("delete", plugin_opts);
  REQUIRE_NOERROR(identity_step);
  transform->add_step(std::move(*identity_step));
  auto transformer = self->spawn(vast::system::partition_transformer, uuid,
                                 store_id, synopsis_opts, index_opts, importer,
                                 filesystem, std::move(transform));
  REQUIRE(transformer);
  // Stream data
  auto partition_path = std::filesystem::path{"/partition.fbs"};
  auto synopsis_path = std::filesystem::path{"/partition_synopsis.fbs"};
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v,
                          partition_path, synopsis_path);
  run();
  size_t events = 0;
  for (auto& slice : zeek_conn_log) {
    events += slice.rows();
    self->send(transformer, slice);
  }
  self->send(transformer, vast::atom::done_v);
  run();
  std::shared_ptr<vast::partition_synopsis> synopsis = nullptr;
  rp.receive(
    [&](std::shared_ptr<vast::partition_synopsis>& ps) {
      REQUIRE(ps);
      synopsis = ps;
    },
    [&](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  // Verify serialized data
  auto partition_rp = self->request(filesystem, caf::infinite,
                                    vast::atom::read_v, partition_path);
  auto synopsis_rp = self->request(filesystem, caf::infinite,
                                   vast::atom::read_v, synopsis_path);
  run();
  partition_rp.receive(
    [&](vast::chunk_ptr& partition_chunk) {
      REQUIRE(partition_chunk);
      const auto* partition = vast::fbs::GetPartition(partition_chunk->data());
      REQUIRE_EQUAL(partition->partition_type(),
                    vast::fbs::partition::Partition::v0);
      const auto* partition_v0 = partition->partition_as_v0();
      // TODO: Implement a new transform step that deletes
      // whole events, as opposed to specific fields.
      CHECK_EQUAL(partition_v0->events(), events);
      vast::legacy_record_type combined_layout;
      REQUIRE(!vast::fbs::deserialize_bytes(partition_v0->combined_layout(),
                                            combined_layout));
      // Verify that the deleted column does not exist anymore.
      const auto* column = combined_layout.find("zeek.conn.uid");
      CHECK(!column);
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  synopsis_rp.receive(
    [&](vast::chunk_ptr& synopsis_chunk) {
      REQUIRE(synopsis_chunk);
      const auto* partition
        = vast::fbs::GetPartitionSynopsis(synopsis_chunk->data());
      REQUIRE_EQUAL(partition->partition_synopsis_type(),
                    vast::fbs::partition_synopsis::PartitionSynopsis::v0);
      const auto* synopsis_v0 = partition->partition_synopsis_as_v0();
      CHECK_EQUAL(synopsis_v0->id_range()->begin(), IDSPACE_BEGIN);
      CHECK_EQUAL(synopsis_v0->id_range()->end(), IDSPACE_BEGIN + events);
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
}

TEST(partition transform via the index) {
  // Spawn index and fill with data
  auto index_dir = std::filesystem::path{"/vast/index"};
  auto archive = self->spawn(mock_archive);
  const auto partition_capacity = 8;
  const auto in_mem_partitions = 10;
  const auto taste_count = 1;
  const auto num_query_supervisors = 10;
  const auto meta_index_fp_rate = 0.01;
  auto index
    = self->spawn(vast::system::index, filesystem, archive, index_dir,
                  vast::defaults::system::store_backend, partition_capacity,
                  in_mem_partitions, taste_count, num_query_supervisors,
                  index_dir, meta_index_fp_rate);
  self->send(index, vast::atom::importer_v, importer);
  vast::detail::spawn_container_source(sys, zeek_conn_log, index);
  run();
  // Get one of the partitions that were persisted.
  auto rp = self->request(filesystem, caf::infinite, vast::atom::read_v,
                          index_dir / "index.bin");
  run();
  vast::uuid partition_uuid = {};
  rp.receive(
    [&](vast::chunk_ptr& index_chunk) {
      REQUIRE(index_chunk);
      const auto* index = vast::fbs::GetIndex(index_chunk->data());
      REQUIRE_EQUAL(index->index_type(), vast::fbs::index::Index::v0);
      const auto* index_v0 = index->index_as_v0();
      const auto* partition_uuids = index_v0->partitions();
      REQUIRE(partition_uuids);
      REQUIRE_GREATER(partition_uuids->size(), 0ull);
      const auto* uuid_fb = *partition_uuids->begin();
      VAST_ASSERT(uuid_fb);
      REQUIRE_EQUAL(unpack(*uuid_fb, partition_uuid), caf::no_error);
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  // Run a partition transformation.
  auto transform = std::make_shared<vast::transform>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_step = vast::make_transform_step("identity", caf::settings{});
  REQUIRE_NOERROR(identity_step);
  transform->add_step(std::move(*identity_step));
  auto rp2 = self->request(index, caf::infinite, vast::atom::apply_v, transform,
                           partition_uuid);
  run();
  rp2.receive(
    [](vast::atom::done) {
      REQUIRE(true);
    },
    [](const caf::error& e) {
      REQUIRE_EQUAL(e, caf::no_error);
    });
  self->send_exit(index, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
