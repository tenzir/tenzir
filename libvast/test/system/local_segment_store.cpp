//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE local_segment_store

#include "vast/atoms.hpp"
#include "vast/chunk.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/query_context.hpp"
#include "vast/segment_store.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/memory_filesystem.hpp"
#include "vast/test/test.hpp"

#include <map>

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    filesystem = self->spawn(memory_filesystem);
  }

  std::vector<vast::table_slice>
  query(const vast::system::store_actor& actor, const vast::ids& ids) {
    bool done = false;
    uint64_t tally = 0;
    uint64_t rows = 0;
    std::vector<vast::table_slice> result;
    auto query_context
      = vast::query_context::make_extract(self, vast::expression{});
    query_context.ids = ids;
    self->send(actor, vast::atom::query_v, query_context);
    run();
    std::this_thread::sleep_for(std::chrono::seconds{1});

    self
      ->do_receive(
        [&](uint64_t x) {
          tally = x;
          done = true;
        },
        [&](vast::atom::receive, vast::table_slice slice) {
          rows += slice.rows();
          result.push_back(std::move(slice));
        })
      .until(done);
    REQUIRE_EQUAL(rows, tally);
    return result;
  }

  vast::system::accountant_actor accountant = {};
  vast::system::filesystem_actor filesystem;
};

} // namespace

TEST(different uuids produce different paths) {
  auto uuid1 = vast::uuid::random();
  auto uuid2 = vast::uuid::random();
  auto path1 = vast::store_path_for_partition(uuid1);
  auto path2 = vast::store_path_for_partition(uuid2);
  CHECK_NOT_EQUAL(path1, path2);
}

FIXTURE_SCOPE(filesystem_tests, fixture)

TEST(local store roundtrip) {
  auto xs = std::vector<vast::table_slice>{zeek_conn_log[0]};
  auto uuid = vast::uuid::random();
  auto plugin = vast::plugins::find<vast::store_actor_plugin>("segment-store");
  REQUIRE(plugin);
  auto builder_and_header
    = plugin->make_store_builder(accountant, filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  vast::detail::spawn_container_source(sys, xs, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(accountant, filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto ids = vast::make_ids({0});
  auto results = query(*store, ids);
  run();
  CHECK_EQUAL(results.size(), 1ull);
  auto expected_rows = select(xs[0], ids);
  CHECK_EQUAL(results[0].rows(), expected_rows[0].rows());
}

FIXTURE_SCOPE_END()
