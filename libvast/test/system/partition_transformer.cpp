//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE partition_transformer

#include "vast/system/partition_transformer.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/format/zeek.hpp"
#include "vast/legacy_type.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/pipeline.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/index.hpp"
#include "vast/system/type_registry.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/memory_filesystem.hpp"
#include "vast/test/test.hpp"

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

using namespace std::string_literals;

namespace {

auto constexpr const IDSPACE_BEGIN = vast::id{0ull};
auto const PARTITION_PATH_TEMPLATE = std::string{"/partition-{}.fbs"};
auto const SYNOPSIS_PATH_TEMPLATE = std::string{"/partition_synopsis-{}.fbs"};

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    filesystem = self->spawn(memory_filesystem);
    auto type_system_path = std::filesystem::path{"/type-registry"};
    type_registry = self->spawn(vast::system::type_registry, type_system_path);
  }

  fixture(const fixture&) = delete;
  fixture(fixture&&) = delete;
  fixture& operator=(const fixture&) = delete;
  fixture& operator=(fixture&&) = delete;

  ~fixture() override {
    self->send_exit(filesystem, caf::exit_reason::user_shutdown);
    self->send_exit(type_registry, caf::exit_reason::user_shutdown);
  }

  vast::system::accountant_actor accountant = {};
  vast::system::type_registry_actor type_registry;
  vast::system::filesystem_actor filesystem;
};

} // namespace

FIXTURE_SCOPE(partition_transformer_tests, fixture)

TEST(identity pipeline / done before persist) {
  // Spawn partition transformer
  auto store_id = "segment-store"s;
  auto synopsis_opts = vast::index_config{};
  auto index_opts = caf::settings{};
  auto pipeline = std::make_shared<vast::pipeline>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_operator
    = vast::make_pipeline_operator("identity", vast::record{});
  REQUIRE_NOERROR(identity_operator);
  pipeline->add_operator(std::move(*identity_operator));
  auto transformer
    = self->spawn(vast::system::partition_transformer, store_id, synopsis_opts,
                  index_opts, accountant, type_registry, filesystem,
                  std::move(pipeline), PARTITION_PATH_TEMPLATE,
                  SYNOPSIS_PATH_TEMPLATE);
  REQUIRE(transformer);
  // Stream data
  size_t events = 0;
  for (auto& slice : zeek_conn_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  self->send(transformer, vast::atom::receive_v, vast::atom::done_v);
  run();
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v);
  run();
  auto synopsis = vast::partition_synopsis_ptr{nullptr};
  auto uuid = vast::uuid::nil();
  rp.receive(
    [&](std::vector<vast::augmented_partition_synopsis>& apsv) {
      REQUIRE_EQUAL(apsv.size(), 1ull);
      auto& aps = apsv.front();
      CHECK_EQUAL(aps.synopsis->events, 20ull);
      CHECK_EQUAL(aps.type.name(), "zeek.conn");
      uuid = aps.uuid;
      synopsis = aps.synopsis;
    },
    [&](caf::error& err) {
      FAIL("failed to persist: " << err);
    });
  // Verify serialized data
  auto partition_path
    = fmt::format(VAST_FMT_RUNTIME(PARTITION_PATH_TEMPLATE), uuid);
  auto synopsis_path
    = fmt::format(VAST_FMT_RUNTIME(SYNOPSIS_PATH_TEMPLATE), uuid);
  auto partition_rp
    = self->request(filesystem, caf::infinite, vast::atom::read_v,
                    std::filesystem::path{partition_path});
  auto synopsis_rp
    = self->request(filesystem, caf::infinite, vast::atom::read_v,
                    std::filesystem::path{synopsis_path});
  run();
  partition_rp.receive(
    [&](vast::chunk_ptr& partition_chunk) {
      REQUIRE(partition_chunk);
      const auto* partition = vast::fbs::GetPartition(partition_chunk->data());
      REQUIRE_EQUAL(partition->partition_type(),
                    vast::fbs::partition::Partition::legacy);
      const auto* partition_legacy = partition->partition_as_legacy();
      CHECK_EQUAL(partition_legacy->events(), events);
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
                    vast::fbs::partition_synopsis::PartitionSynopsis::legacy);
      const auto* synopsis_legacy = partition->partition_synopsis_as_legacy();
      CHECK_EQUAL(synopsis_legacy->id_range()->begin(), IDSPACE_BEGIN);
      CHECK_EQUAL(synopsis_legacy->id_range()->end(), IDSPACE_BEGIN + events);
    },
    [](const caf::error&) {
      FAIL("failed to read stored synopsis");
    });
}

TEST(delete pipeline / persist before done) {
  // Spawn partition transformer
  auto store_id = "segment-store"s;
  auto synopsis_opts = vast::index_config{};
  auto index_opts = caf::settings{};
  auto pipeline = std::make_shared<vast::pipeline>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto delete_operator_config = vast::record{{"fields", vast::list{"uid"}}};
  auto delete_operator
    = vast::make_pipeline_operator("drop", delete_operator_config);
  REQUIRE_NOERROR(delete_operator);
  pipeline->add_operator(std::move(*delete_operator));
  auto transformer
    = self->spawn(vast::system::partition_transformer, store_id, synopsis_opts,
                  index_opts, accountant, type_registry, filesystem,
                  std::move(pipeline), PARTITION_PATH_TEMPLATE,
                  SYNOPSIS_PATH_TEMPLATE);
  REQUIRE(transformer);
  // Stream data
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v);
  run();
  size_t events = 0;
  for (auto& slice : zeek_conn_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  self->send(transformer, vast::atom::receive_v, vast::atom::done_v);
  run();
  auto synopsis = vast::partition_synopsis_ptr{nullptr};
  auto uuid = vast::uuid::nil();
  rp.receive(
    [&](std::vector<vast::augmented_partition_synopsis>& apsv) {
      REQUIRE_EQUAL(apsv.size(), 1ull);
      auto& aps = apsv.front();
      REQUIRE(aps.synopsis);
      synopsis = aps.synopsis;
      uuid = aps.uuid;
    },
    [&](const caf::error& e) {
      FAIL("unexpected error" << e);
    });
  // Verify serialized data
  auto partition_path
    = fmt::format(VAST_FMT_RUNTIME(PARTITION_PATH_TEMPLATE), uuid);
  auto synopsis_path
    = fmt::format(VAST_FMT_RUNTIME(SYNOPSIS_PATH_TEMPLATE), uuid);
  auto partition_rp
    = self->request(filesystem, caf::infinite, vast::atom::read_v,
                    std::filesystem::path{partition_path});
  auto synopsis_rp
    = self->request(filesystem, caf::infinite, vast::atom::read_v,
                    std::filesystem::path{synopsis_path});
  run();
  partition_rp.receive(
    [&](vast::chunk_ptr& partition_chunk) {
      REQUIRE(partition_chunk);
      const auto* partition = vast::fbs::GetPartition(partition_chunk->data());
      REQUIRE_EQUAL(partition->partition_type(),
                    vast::fbs::partition::Partition::legacy);
      const auto* partition_legacy = partition->partition_as_legacy();
      // TODO: Implement a new pipeline operator that deletes
      // whole events, as opposed to specific fields.
      CHECK_EQUAL(partition_legacy->events(), events);
      vast::legacy_record_type intermediate;
      REQUIRE(!vast::fbs::deserialize_bytes(partition_legacy->combined_layout(),
                                            intermediate));
      auto combined_layout = vast::type::from_legacy_type(intermediate);
      REQUIRE(caf::holds_alternative<vast::record_type>(combined_layout));
      // Verify that the deleted column does not exist anymore.
      const auto column = caf::get<vast::record_type>(combined_layout)
                            .resolve_key("zeek.conn.uid");
      CHECK(!column.has_value());
    },
    [](const caf::error& e) {
      FAIL("unexpected error " << e);
    });
  synopsis_rp.receive(
    [&](vast::chunk_ptr& synopsis_chunk) {
      REQUIRE(synopsis_chunk);
      const auto* partition
        = vast::fbs::GetPartitionSynopsis(synopsis_chunk->data());
      REQUIRE_EQUAL(partition->partition_synopsis_type(),
                    vast::fbs::partition_synopsis::PartitionSynopsis::legacy);
      const auto* synopsis_legacy = partition->partition_synopsis_as_legacy();
      CHECK_EQUAL(synopsis_legacy->id_range()->begin(), IDSPACE_BEGIN);
      CHECK_EQUAL(synopsis_legacy->id_range()->end(), IDSPACE_BEGIN + events);
    },
    [](const caf::error& e) {
      FAIL("unexpected error " << e);
    });
}

TEST(partition with multiple types) {
  // Spawn partition transformer
  auto store_id = "segment-store"s;
  auto synopsis_opts = vast::index_config{};
  auto index_opts = caf::settings{};
  auto pipeline = std::make_shared<vast::pipeline>("partition_transform"s,
                                                   std::vector<std::string>{});
  auto identity_operator
    = vast::make_pipeline_operator("identity", vast::record{});
  REQUIRE_NOERROR(identity_operator);
  pipeline->add_operator(std::move(*identity_operator));
  auto transformer
    = self->spawn(vast::system::partition_transformer, store_id, synopsis_opts,
                  index_opts, accountant, type_registry, filesystem,
                  std::move(pipeline), PARTITION_PATH_TEMPLATE,
                  SYNOPSIS_PATH_TEMPLATE);
  REQUIRE(transformer);
  // Stream data with three different types
  size_t events = 0;
  for (auto& slice : suricata_dns_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  for (auto& slice : suricata_http_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  for (auto& slice : suricata_dns_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  for (auto& slice : suricata_flow_log) {
    events += slice.rows();
    self->send(transformer, vast::atom::receive_v, slice);
  }
  self->send(transformer, vast::atom::receive_v, vast::atom::done_v);
  run();
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v);
  run();
  auto synopses = std::vector<vast::partition_synopsis_ptr>{};
  auto uuids = std::vector<vast::uuid>{};
  auto stats = vast::index_statistics{};
  rp.receive(
    [&](std::vector<vast::augmented_partition_synopsis>& apsv) {
      CHECK_EQUAL(apsv.size(), 3ull);
      for (auto& aps : apsv) {
        stats.layouts[std::string{aps.type.name()}].count
          += aps.synopsis->events;
        uuids.push_back(aps.uuid);
        synopses.push_back(aps.synopsis);
      }
    },
    [&](caf::error& err) {
      FAIL("failed to persist: " << err);
    });
  CHECK_EQUAL(stats.layouts["suricata.dns"].count, 2ull);
  CHECK_EQUAL(stats.layouts["suricata.flow"].count, 1ull);
  CHECK_EQUAL(stats.layouts["suricata.http"].count, 1ull);
  size_t total_count = 0ull;
  for (auto const& [layout, stats] : stats.layouts)
    total_count += stats.count;
  CHECK_EQUAL(total_count, events);
  // Verify that the partitions exist on disk.
  for (auto& uuid : uuids) {
    auto partition_path
      = fmt::format(VAST_FMT_RUNTIME(PARTITION_PATH_TEMPLATE), uuid);
    auto synopsis_path
      = fmt::format(VAST_FMT_RUNTIME(SYNOPSIS_PATH_TEMPLATE), uuid);
    auto partition_rp
      = self->request(filesystem, caf::infinite, vast::atom::read_v,
                      std::filesystem::path{partition_path});
    auto synopsis_rp
      = self->request(filesystem, caf::infinite, vast::atom::read_v,
                      std::filesystem::path{synopsis_path});
    run();
    partition_rp.receive(
      [&](vast::chunk_ptr& partition_chunk) {
        CHECK(partition_chunk);
      },
      [&](const caf::error&) {
        FAIL("failed to read partition " << uuid);
      });
    synopsis_rp.receive(
      [&](vast::chunk_ptr& synopsis_chunk) {
        CHECK(synopsis_chunk);
      },
      [&](const caf::error&) {
        FAIL("failed to read synopsis " << uuid);
      });
  }
}

TEST(identity partition pipeline via the index) {
  // Spawn index and fill with data
  auto index_dir = std::filesystem::path{"/vast/index"};
  auto archive = vast::system::archive_actor{};
  auto catalog = self->spawn(vast::system::catalog, accountant);
  const auto partition_capacity = 8;
  const auto active_partition_timeout = vast::duration{};
  const auto in_mem_partitions = 10;
  const auto taste_count = 1;
  const auto num_query_supervisors = 10;
  const auto index_config = vast::index_config{};
  auto index
    = self->spawn(vast::system::index, accountant, filesystem, archive, catalog,
                  type_registry, index_dir,
                  vast::defaults::system::store_backend, partition_capacity,
                  active_partition_timeout, in_mem_partitions, taste_count,
                  num_query_supervisors, index_dir, index_config);
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
      FAIL("unexpected error" << e);
    });
  // Check how big the partition is.
  auto rp2 = self->request(filesystem, caf::infinite, vast::atom::read_v,
                           index_dir / fmt::format("{}.mdx", partition_uuid));
  run();
  size_t events = 0;
  rp2.receive(
    [&](vast::chunk_ptr& partition_synopsis_chunk) {
      REQUIRE(partition_synopsis_chunk);
      const auto* partition_synopsis
        = vast::fbs::GetPartitionSynopsis(partition_synopsis_chunk->data());
      REQUIRE_EQUAL(partition_synopsis->partition_synopsis_type(),
                    vast::fbs::partition_synopsis::PartitionSynopsis::legacy);
      const auto* partition_synopsis_legacy
        = partition_synopsis->partition_synopsis_as_legacy();
      const auto* range = partition_synopsis_legacy->id_range();
      events = range->end() - range->begin();
    },
    [](const caf::error& e) {
      REQUIRE_SUCCESS(e);
    });
  // Run a partition transformation.
  auto pipeline = std::make_shared<vast::pipeline>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_operator
    = vast::make_pipeline_operator("identity", vast::record{});
  REQUIRE_NOERROR(identity_operator);
  pipeline->add_operator(std::move(*identity_operator));
  auto rp3 = self->request(index, caf::infinite, vast::atom::apply_v, pipeline,
                           std::vector<vast::uuid>{partition_uuid},
                           vast::system::keep_original_partition::yes);
  run();
  rp3.receive(
    [=](const std::vector<vast::partition_info>& infos) {
      REQUIRE_EQUAL(infos.size(), 1ull);
      CHECK_EQUAL(infos[0].events, events);
    },
    [](const caf::error& e) {
      FAIL("unexpected error" << e);
    });
  auto rp4 = self->request(filesystem, caf::infinite, vast::atom::read_v,
                           index_dir / fmt::format("{}.mdx", partition_uuid));
  run();
  rp4.receive([&](vast::chunk_ptr&) {},
              [](const caf::error& e) {
                REQUIRE_SUCCESS(e);
              });
  auto rp5 = self->request(index, caf::infinite, vast::atom::apply_v, pipeline,
                           std::vector<vast::uuid>{partition_uuid},
                           vast::system::keep_original_partition::no);
  run();
  rp5.receive(
    [=](const std::vector<vast::partition_info>& infos) {
      REQUIRE_EQUAL(infos.size(), 1ull);
      CHECK_EQUAL(infos[0].events, events);
    },
    [](const caf::error& e) {
      FAIL("unexpected error" << e);
    });
  self->send_exit(index, caf::exit_reason::user_shutdown);
}

TEST(select pipeline with an empty result set) {
  // Spawn index and fill with data
  auto index_dir = std::filesystem::path{"/vast/index"};
  auto archive = vast::system::archive_actor{};
  auto catalog = self->spawn(vast::system::catalog, accountant);
  const auto partition_capacity = 8;
  const auto active_partition_timeout = vast::duration{};
  const auto in_mem_partitions = 10;
  const auto taste_count = 1;
  const auto num_query_supervisors = 10;
  auto index
    = self->spawn(vast::system::index, accountant, filesystem, archive, catalog,
                  type_registry, index_dir,
                  vast::defaults::system::store_backend, partition_capacity,
                  active_partition_timeout, in_mem_partitions, taste_count,
                  num_query_supervisors, index_dir, vast::index_config{});
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
      FAIL("unexpected error" << e);
    });
  // Run a partition transformation.
  auto pipeline = std::make_shared<vast::pipeline>(
    "partition_transform"s, std::vector<std::string>{"zeek.conn"});
  auto identity_operator_config
    = vast::record{{"expression", "#type == \"does_not_exist\""}};
  auto identity_operator
    = vast::make_pipeline_operator("where", identity_operator_config);
  REQUIRE_NOERROR(identity_operator);
  pipeline->add_operator(std::move(*identity_operator));
  auto rp2 = self->request(index, caf::infinite, vast::atom::apply_v, pipeline,
                           std::vector<vast::uuid>{partition_uuid},
                           vast::system::keep_original_partition::no);
  run();
  rp2.receive(
    [=](const std::vector<vast::partition_info>& infos) {
      CHECK_EQUAL(infos.size(), 0ull);
    },
    [](const caf::error& e) {
      FAIL("unexpected error" << e);
    });
  self->send_exit(index, caf::exit_reason::user_shutdown);
}

// Test that the partition transformer outputs multiple partitions
// when it gets so many events that they exceed the maximum size
// for a single partition.
TEST(exceeded partition size) {
  // Spawn partition transformer with a small max partition size.
  auto store_id = "segment-store"s;
  auto synopsis_opts = vast::index_config{};
  auto index_opts = caf::settings{};
  index_opts["cardinality"] = 4;
  auto pipeline = std::make_shared<vast::pipeline>("partition_transform"s,
                                                   std::vector<std::string>{});
  auto identity_operator
    = vast::make_pipeline_operator("identity", vast::record{});
  REQUIRE_NOERROR(identity_operator);
  pipeline->add_operator(std::move(*identity_operator));
  auto transformer
    = self->spawn(vast::system::partition_transformer, store_id, synopsis_opts,
                  index_opts, accountant, type_registry, filesystem,
                  std::move(pipeline), PARTITION_PATH_TEMPLATE,
                  SYNOPSIS_PATH_TEMPLATE);
  REQUIRE(transformer);
  // Stream data with three different types
  size_t const expected_total = 8ull;
  size_t events = 0;
  for (size_t i = 0; i < expected_total; ++i) {
    for (auto& slice : suricata_dns_log) {
      events += slice.rows();
      self->send(transformer, vast::atom::receive_v, slice);
    }
  }
  CHECK_EQUAL(events, expected_total);
  self->send(transformer, vast::atom::receive_v, vast::atom::done_v);
  run();
  auto rp = self->request(transformer, caf::infinite, vast::atom::persist_v);
  run();
  auto synopses = std::vector<vast::partition_synopsis_ptr>{};
  auto uuids = std::vector<vast::uuid>{};
  auto stats = vast::index_statistics{};
  rp.receive(
    [&](std::vector<vast::augmented_partition_synopsis>& apsv) {
      // We expect to receive 2 partitions with 4 events each.
      CHECK_EQUAL(apsv.size(), 2ull);
      for (auto& aps : apsv) {
        stats.layouts[std::string{aps.type.name()}].count
          += aps.synopsis->events;
      }
      CHECK_EQUAL(stats.layouts["suricata.dns"].count, 8ull);
    },
    [&](caf::error& err) {
      FAIL("failed to persist: " << err);
    });
}

FIXTURE_SCOPE_END()
