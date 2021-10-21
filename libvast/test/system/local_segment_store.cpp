//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE local_segment_store

#include "vast/system/local_segment_store.hpp"

#include "vast/chunk.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/query.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <map>

namespace {

// An in-memory implementation of the filesystem actor.
vast::system::filesystem_actor::behavior_type memory_filesystem() {
  auto chunks
    = std::make_shared<std::map<std::filesystem::path, vast::chunk_ptr>>();
  return {
    [chunks](vast::atom::write, std::filesystem::path path,
             vast::chunk_ptr chunk) {
      (*chunks)[path] = chunk;
      return vast::atom::ok_v;
    },
    [chunks](vast::atom::read,
             std::filesystem::path path) -> caf::result<vast::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(vast::ec::filesystem_error, "unknown file");
      return chunk->second;
    },
    [chunks](vast::atom::mmap,
             std::filesystem::path path) -> caf::result<vast::chunk_ptr> {
      auto chunk = chunks->find(path);
      if (chunk == chunks->end())
        return caf::make_error(vast::ec::filesystem_error, "unknown file");
      return chunk->second;
    },
    [](vast::atom::erase, std::filesystem::path&) {
      return vast::atom::done_v;
    },
    [](vast::atom::status, vast::system::status_verbosity) -> vast::record {
      return {};
    },
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    filesystem = self->spawn(memory_filesystem);
  }

  std::vector<vast::table_slice>
  query(vast::system::store_actor actor, const vast::ids& ids) {
    bool done = false;
    std::vector<vast::table_slice> result;
    auto query = vast::query::make_extract(
      self, vast::query::extract::preserve_ids, vast::expression{});
    query.ids = ids;
    self->send(actor, query);
    run();
    std::this_thread::sleep_for(std::chrono::seconds{1});

    self
      ->do_receive(
        [&](vast::atom::done) {
          done = true;
        },
        [&](vast::table_slice slice) {
          result.push_back(std::move(slice));
        })
      .until(done);
    return result;
  }

  vast::system::filesystem_actor filesystem;
};

} // namespace

TEST(different uuids produce different paths) {
  auto uuid1 = vast::uuid::random();
  auto uuid2 = vast::uuid::random();
  auto path1 = vast::system::store_path_for_partition(uuid1);
  auto path2 = vast::system::store_path_for_partition(uuid2);
  CHECK_NOT_EQUAL(path1, path2);
}

FIXTURE_SCOPE(filesystem_tests, fixture)

TEST(local store roundtrip) {
  auto xs = std::vector<vast::table_slice>{zeek_conn_log[0]};
  xs[0].offset(23u);
  auto uuid = vast::uuid::random();
  auto plugin = vast::plugins::find<vast::store_plugin>("segment-store");
  REQUIRE(plugin);
  auto builder_and_header = plugin->make_store_builder(filesystem, uuid);
  REQUIRE_NOERROR(builder_and_header);
  auto& [builder, header] = *builder_and_header;
  vast::detail::spawn_container_source(sys, xs, builder);
  run();
  // The local store expects a single stream source, so the data should be
  // flushed to disk after the source disconnected.
  auto store = plugin->make_store(filesystem, as_bytes(header));
  REQUIRE_NOERROR(store);
  run();
  auto ids = vast::make_ids({23});
  auto results = query(*store, ids);
  run();
  CHECK_EQUAL(results.size(), 1ull);
  CHECK_EQUAL(results[0].offset(), 23ull);
  auto expected_rows = select(xs[0], ids);
  CHECK_EQUAL(results[0].rows(), expected_rows[0].rows());
}

FIXTURE_SCOPE_END()
