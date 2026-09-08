//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/export_bridge.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/partition_paths.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/posix_filesystem.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_registry.hpp>
#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/test/fixture/deterministic.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

using namespace tenzir;
using namespace std::chrono_literals;

namespace {

constexpr auto timeout = std::chrono::seconds{10};

/// A catalog actor over a scratch database directory, plus the helpers to put
/// partitions into it and to check what is left on disk.
struct fixture {
  /// How long an erased-but-pinned partition may linger. Zero never forces.
  explicit fixture(duration deferred_erase_timeout = duration::zero(),
                   maintenance_options maintenance = {})
    : deferred_erase_timeout{deferred_erase_timeout} {
    factory<synopsis>::initialize();
    std::filesystem::create_directories(paths.index_dir);
    std::filesystem::create_directories(paths.archive_dir);
    start(std::move(maintenance));
  }

  void start(maintenance_options maintenance = {}) {
    fs = sys.spawn(posix_filesystem, dbdir);
    catalog = sys.spawn(tenzir::catalog, fs, paths, std::string{"feather"},
                        index_config{}, /*partition_capacity=*/size_t{1024},
                        /*desired_batch_size=*/size_t{1024},
                        std::move(maintenance), deferred_erase_timeout,
                        /*sketch_cache_bytes=*/size_t{0},
                        /*lazy_sketches=*/false,
                        /*lookup_parallelism=*/size_t{1}, node_actor{});
  }

  duration deferred_erase_timeout = {};
  caf::actor_system_config config = {};
  caf::actor_system sys{config};
  std::filesystem::path dbdir
    = std::filesystem::temp_directory_path()
      / fmt::format("tnz-catalog-leases-{}", uuid::random());
  partition_paths paths = partition_paths::from_database_dir(dbdir);
  filesystem_actor fs = {};
  catalog_actor catalog = {};
  type schema = type{"test", record_type{{"msg", string_type{}}}};

  ~fixture() {
    caf::anon_send_exit(catalog, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(fs, caf::exit_reason::user_shutdown);
    auto ec = std::error_code{};
    std::filesystem::remove_all(dbdir, ec);
  }

  fixture(const fixture&) = delete;
  fixture(fixture&&) = delete;
  auto operator=(const fixture&) -> fixture& = delete;
  auto operator=(fixture&&) -> fixture& = delete;

  auto await_shutdown() -> bool {
    caf::anon_send_exit(catalog, caf::exit_reason::user_shutdown);
    caf::anon_send_exit(fs, caf::exit_reason::user_shutdown);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (sys.running_actors_count() == 0) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  }

  /// Writes the three files of a partition and merges its synopsis into the
  /// catalog. The contents do not matter: erasure only ever deletes the paths
  /// the synopsis names.
  auto add_partition() -> uuid {
    auto id = uuid::random();
    for (const auto& path : files_of(id)) {
      auto out = std::ofstream{path};
      out << fmt::format("partition {}", id);
    }
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().schema = schema;
    synopsis.unshared().events = 1;
    // The catalog answers `#schema`-based predicates — which is what a
    // trivially true expression boils down to — by walking the field
    // synopses, so a partition without any is invisible to every lookup.
    synopsis.unshared().field_synopses_[qualified_record_field{
      "test", "msg", type{string_type{}}}]
      = nullptr;
    synopsis.unshared().min_import_time = time::min();
    synopsis.unshared().max_import_time = time::min();
    synopsis.unshared().indexes_file = {
      .url = fmt::format("file://{}", paths.partition(id).string()), .size = 0};
    synopsis.unshared().sketches_file = {
      .url = fmt::format("file://{}", paths.synopsis(id).string()), .size = 0};
    synopsis.unshared().store_file
      = {.url = fmt::format("file://{}", store_of(id).string()), .size = 0};
    auto self = caf::scoped_actor{sys};
    auto merged = false;
    self
      ->mail(atom::merge_v,
             std::vector<partition_synopsis_pair>{{id, std::move(synopsis)}})
      .request(catalog, timeout)
      .receive(
        [&](atom::ok) {
          merged = true;
        },
        [](const caf::error& err) {
          FAIL("failed to merge partition: {}", err);
        });
    REQUIRE(merged);
    return id;
  }

  auto store_of(const uuid& id) const -> std::filesystem::path {
    return paths.archive_dir / fmt::format("{:l}.feather", id);
  }

  auto files_of(const uuid& id) const -> std::vector<std::filesystem::path> {
    return {paths.partition(id), paths.synopsis(id), store_of(id)};
  }

  auto files_exist(const uuid& id) const -> bool {
    return std::ranges::any_of(files_of(id), [](const auto& path) {
      auto ec = std::error_code{};
      return std::filesystem::exists(path, ec);
    });
  }

  /// Waits for the files of a partition to disappear. Deletion runs through
  /// the filesystem actor, so it always trails the request that triggered it.
  auto await_deletion(const uuid& id) const -> bool {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (not files_exist(id)) {
        return true;
      }
      std::this_thread::sleep_for(10ms);
    }
    return false;
  }

  /// Asks for candidates, pinning the result under `query`.
  auto candidates(caf::scoped_actor& self, const uuid& query)
    -> catalog_lookup_result {
    auto context = query_context::make_extract("test", self, expression{});
    context.id = query;
    auto result = catalog_lookup_result{};
    self->mail(atom::candidates_v, std::move(context))
      .request(catalog, timeout)
      .receive(
        [&](catalog_lookup_result& candidates) {
          result = std::move(candidates);
        },
        [](const caf::error& err) {
          FAIL("candidate lookup failed: {}", err);
        });
    return result;
  }

  /// Releases one partition from a candidate set.
  void release(caf::scoped_actor& self, const uuid& query, const uuid& id) {
    auto released = false;
    self->mail(atom::release_v, query, std::vector{id})
      .request(catalog, timeout)
      .receive(
        [&] {
          released = true;
        },
        [](const caf::error& err) {
          FAIL("release failed: {}", err);
        });
    REQUIRE(released);
  }

  void release(caf::scoped_actor& self, const uuid& query) {
    auto released = false;
    self->mail(atom::release_v, query)
      .request(catalog, timeout)
      .receive(
        [&] {
          released = true;
        },
        [](const caf::error& err) {
          FAIL("release failed: {}", err);
        });
    REQUIRE(released);
  }

  /// Erases a partition, returning the error if the catalog refused.
  auto erase(caf::scoped_actor& self, const uuid& id) -> caf::error {
    auto result = caf::error{};
    self->mail(atom::erase_v, id)
      .request(catalog, timeout)
      .receive(
        [](atom::done) {
          // nop
        },
        [&](caf::error& err) {
          result = std::move(err);
        });
    return result;
  }

  /// Collects the ids in a candidate set.
  static auto ids_of(const catalog_lookup_result& candidates)
    -> std::vector<uuid> {
    auto result = std::vector<uuid>{};
    for (const auto& [type, info] : candidates.candidate_infos) {
      for (const auto& partition : info.partition_infos) {
        result.push_back(partition.uuid);
      }
    }
    std::ranges::sort(result);
    return result;
  }
};

} // namespace

TEST("catalog shutdown stops its lookup workers") {
  auto f = fixture{};
  f.add_partition();
  CHECK(f.await_shutdown());
}

TEST("catalog startup failure stops its lookup workers") {
  auto maintenance = maintenance_options{};
  maintenance.rebuild_timezone = "Not/A-Timezone";
  auto f = fixture{duration::zero(), std::move(maintenance)};
  {
    auto self = caf::scoped_actor{f.sys};
    self->mail(atom::status_v, status_verbosity::info, duration::zero())
      .request(f.catalog, timeout)
      .receive(
        [](const record&) {
          FAIL("catalog accepted an invalid timezone");
        },
        [](const caf::error& error) {
          CHECK(error.valid());
        });
  }
  CHECK(f.await_shutdown());
}

TEST("a disk scan exits without its catalog processing the response") {
  auto f = fixture{};
  f.sys.spawn(
    [paths = f.paths](catalog_actor::stateful_pointer<catalog_state> self)
      -> catalog_actor::behavior_type {
      self->state().self = self;
      self->state().paths = paths;
      self->state().measure_space();
      // Quit during initialization, before any response continuation can run.
      self->quit();
      return catalog_actor::behavior_type::make_empty_behavior();
    });
  CHECK(f.await_shutdown());
}

TEST("export releases unbound schemas while other candidates remain queued") {
  auto f = caf::test::fixture::deterministic{};
  const auto skipped = uuid::random();
  const auto queued = uuid::random();
  const auto good = type{"good", record_type{{"msg", string_type{}}}};
  const auto bad = type{"bad", record_type{{"other", string_type{}}}};
  const auto expr
    = expression{predicate{field_extractor{"msg"}, relational_operator::equal,
                           data{std::string{"value"}}}};
  auto lease = uuid{};
  auto released = std::vector<uuid>{};
  auto released_all = false;
  auto catalog = f.sys.spawn([&]() -> catalog_actor::behavior_type {
    return {
      caf::partial_behavior_init,
      [&](atom::candidates, const query_context& query) {
        lease = query.id;
        auto result = catalog_lookup_result{};
        result.candidate_infos[good]
          = {expr, {{queued, 1, tenzir::time{}, good, 0}}};
        result.candidate_infos[bad]
          = {expr, {{skipped, 1, tenzir::time{}, bad, 0}}};
        return result;
      },
      [&](atom::release, const uuid& id, const std::vector<uuid>& ids) {
        CHECK_EQUAL(id, lease);
        released.insert(released.end(), ids.begin(), ids.end());
      },
      [&](atom::release, const uuid&) {
        released_all = true;
      },
    };
  });
  auto importer = f.sys.spawn([]() -> importer_actor::behavior_type {
    return {caf::partial_behavior_init,
            [](atom::get, const receiver_actor<table_slice>&, bool, bool, bool,
               bool) {
              return std::vector<table_slice>{};
            }};
  });
  auto fs = f.sys.spawn([]() -> filesystem_actor::behavior_type {
    return {caf::partial_behavior_init,
            [](atom::erase, const std::filesystem::path&) {
              return atom::done_v;
            }};
  });
  f.sys.registry().put("tenzir.catalog", catalog);
  f.sys.registry().put("tenzir.importer", importer);
  auto mode = export_mode{};
  mode.internal = true;
  // Keep valid candidates queued without scheduling any partition reads.
  mode.parallel = 0;
  auto bridge = spawn_export_bridge(
    f.sys, expr, mode, fs, std::make_unique<null_diagnostic_handler>());
  f.dispatch_messages();
  CHECK_NOT_EQUAL(lease, uuid{});
  CHECK_EQUAL(released, std::vector{skipped});
  CHECK(not released_all);
  f.inject_exit(bridge);
  f.inject_exit(catalog);
  f.inject_exit(importer);
  f.inject_exit(fs);
}

TEST("marker finalization retains claims through failed writes") {
  auto f = caf::test::fixture::deterministic{};
  auto writes = size_t{0};
  auto finalized = false;
  auto* state = static_cast<catalog_state*>(nullptr);
  const auto output = uuid::random();
  const auto marker = std::filesystem::path{"test.marker"};
  auto fs = f.sys.spawn([&]() -> filesystem_actor::behavior_type {
    return {caf::partial_behavior_init,
            [&](atom::write, const std::filesystem::path&,
                const chunk_ptr&) -> caf::result<atom::ok> {
              if (++writes < 3) {
                return caf::make_error(ec::filesystem_error,
                                       "injected failure");
              }
              return atom::ok_v;
            }};
  });
  auto catalog
    = f.sys.spawn([&](catalog_actor::stateful_pointer<catalog_state> self)
                    -> catalog_actor::behavior_type {
        state = &self->state();
        state->self = self;
        state->filesystem = fs;
        state->markers_in_disposal[marker] = 1;
        state->in_transformation.insert(output);
        state->finalize_marker(marker, chunk::copy(std::string{"finalized"}),
                               [&, self] {
                                 finalized = true;
                                 self->state().in_transformation.erase(output);
                               });
        return {caf::partial_behavior_init,
                [](atom::status, status_verbosity, duration) {
                  return record{};
                }};
      });
  f.dispatch_messages();
  REQUIRE(state);
  CHECK_EQUAL(writes, size_t{1});
  CHECK(not finalized);
  CHECK(state->in_transformation.contains(output));
  f.advance_time(defaults::disposal_retry_delay);
  f.dispatch_messages();
  CHECK_EQUAL(writes, size_t{2});
  CHECK(not finalized);
  CHECK(state->in_transformation.contains(output));
  f.advance_time(defaults::disposal_retry_delay);
  f.dispatch_messages();
  CHECK_EQUAL(writes, size_t{3});
  CHECK(finalized);
  CHECK(not state->in_transformation.contains(output));
  CHECK(state->marker_referenced(marker));
  f.inject_exit(catalog);
  f.inject_exit(fs);
}

TEST("malformed transform markers refuse startup and retain recovery files") {
  auto f = fixture{};
  const auto input = f.add_partition();
  REQUIRE(f.await_shutdown());
  std::filesystem::create_directories(f.paths.markers_dir);
  const auto marker = f.paths.marker(uuid::random());
  const auto valid = create_marker({input}, {}, keep_original_partition::no);
  auto unknown_builder = flatbuffers::FlatBufferBuilder{};
  fbs::FinishPartitionTransformBuffer(
    unknown_builder, fbs::CreatePartitionTransform(unknown_builder));
  const auto unknown = fbs::release(unknown_builder);
  for (const auto& bytes : {std::span<const std::byte>{},
                            as_bytes(valid).first(4), as_bytes(unknown)}) {
    REQUIRE(not io::save(marker, bytes).valid());
    for (auto restart = 0; restart < 2; ++restart) {
      f.start();
      {
        auto reader = caf::scoped_actor{f.sys};
        reader->mail(atom::status_v, status_verbosity::info, duration::zero())
          .request(f.catalog, timeout)
          .receive(
            [](const record&) {
              FAIL("catalog accepted a malformed transform marker");
            },
            [](const caf::error& error) {
              CHECK(error.valid());
            });
      }
      REQUIRE(f.await_shutdown());
      CHECK_EQUAL(std::filesystem::file_size(marker), bytes.size());
      for (const auto& path : f.files_of(input)) {
        CHECK(std::filesystem::exists(path));
      }
    }
  }
  REQUIRE(not io::save(marker, as_bytes(valid)).valid());
  f.start();
  auto reader = caf::scoped_actor{f.sys};
  CHECK(f.candidates(reader, uuid::random()).empty());
}

TEST("erasure tombstones replay without a policy history invalidation") {
  auto f = fixture{};
  const auto input = f.add_partition();
  REQUIRE(f.await_shutdown());
  std::filesystem::create_directories(f.paths.markers_dir);
  REQUIRE(not io::save(f.paths.marker(uuid::random()),
                       as_bytes(create_marker({input}, {},
                                              keep_original_partition::no)))
                .valid());
  f.start();
  auto reader = caf::scoped_actor{f.sys};
  CHECK(f.candidates(reader, uuid::random()).empty());
  CHECK(not std::filesystem::exists(f.dbdir / invalid_policy_history_path));
}

TEST("failed quarantine replay retains its marker across restarts") {
  auto f = fixture{};
  const auto input = f.add_partition();
  REQUIRE(f.await_shutdown());
  const auto blocker = f.paths.archive_dir / "quarantined";
  {
    // A regular file prevents creation of the quarantine destination directory.
    auto out = std::ofstream{blocker};
    out << "blocked";
  }
  std::filesystem::create_directories(f.paths.markers_dir);
  const auto marker = f.paths.marker(uuid::random());
  REQUIRE(
    not io::save(marker, as_bytes(create_marker(
                           {input}, {}, keep_original_partition::no, true)))
          .valid());
  for (auto attempt = 0; attempt < 2; ++attempt) {
    f.start();
    {
      auto reader = caf::scoped_actor{f.sys};
      CHECK(f.candidates(reader, uuid::random()).empty());
    }
    REQUIRE(f.await_shutdown());
    CHECK(std::filesystem::exists(marker));
    CHECK(std::filesystem::exists(f.store_of(input)));
    CHECK(std::filesystem::exists(f.paths.partition(input)));
  }
  REQUIRE(std::filesystem::remove(blocker));
  f.start();
  {
    auto reader = caf::scoped_actor{f.sys};
    CHECK(f.candidates(reader, uuid::random()).empty());
  }
  CHECK(f.await_deletion(input));
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::filesystem::exists(marker)
         and std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(10ms);
  }
  CHECK(not std::filesystem::exists(marker));
  REQUIRE(f.await_shutdown());
  CHECK(std::filesystem::exists(blocker / f.store_of(input).filename()));
}

TEST("a deduplicated candidate keeps only the queued query's pin") {
  auto f = fixture{};
  const auto id = f.add_partition();
  const auto other = f.add_partition();
  auto reader = caf::scoped_actor{f.sys};
  const auto queued = uuid::random();
  const auto duplicate = uuid::random();
  REQUIRE_EQUAL(fixture::ids_of(f.candidates(reader, queued)).size(),
                size_t{2});
  REQUIRE_EQUAL(fixture::ids_of(f.candidates(reader, duplicate)).size(),
                size_t{2});
  // Deduplication releases the new query's pin, not the queued read's pin.
  f.release(reader, duplicate, id);
  auto eraser = caf::scoped_actor{f.sys};
  CHECK_EQUAL(f.erase(eraser, id), caf::error{});
  CHECK(f.files_exist(id));
  // Finishing the queued read permits disposal while both queries still
  // hold other candidates. No global idle point or owner DOWN is needed.
  f.release(reader, queued, id);
  CHECK(f.await_deletion(id));
  CHECK(f.files_exist(other));
  f.release(reader, queued);
  f.release(reader, duplicate);
}

TEST("a pinned partition keeps its files until the pin is released") {
  auto f = fixture{};
  const auto id = f.add_partition();
  auto reader = caf::scoped_actor{f.sys};
  const auto query = uuid::random();
  CHECK_EQUAL(fixture::ids_of(f.candidates(reader, query)), std::vector{id});
  // The erase succeeds, but the reader still holds the partition, so its files
  // must survive.
  auto eraser = caf::scoped_actor{f.sys};
  CHECK_EQUAL(f.erase(eraser, id), caf::error{});
  CHECK(f.files_exist(id));
  // It is gone from the catalog right away, though.
  CHECK(fixture::ids_of(f.candidates(eraser, uuid::random())).empty());
  f.release(reader, query);
  CHECK(f.await_deletion(id));
}

TEST("a pinned partition is deleted when its reader goes down") {
  auto f = fixture{};
  const auto id = f.add_partition();
  {
    auto reader = caf::scoped_actor{f.sys};
    CHECK_EQUAL(fixture::ids_of(f.candidates(reader, uuid::random())),
                std::vector{id});
    auto eraser = caf::scoped_actor{f.sys};
    CHECK_EQUAL(f.erase(eraser, id), caf::error{});
    CHECK(f.files_exist(id));
  }
  // The reader is gone without ever releasing its candidate set; the catalog
  // notices and cleans up.
  CHECK(f.await_deletion(id));
}

TEST("releasing one partition leaves the rest of the set pinned") {
  auto f = fixture{};
  const auto first = f.add_partition();
  const auto second = f.add_partition();
  auto reader = caf::scoped_actor{f.sys};
  const auto query = uuid::random();
  CHECK_EQUAL(f.candidates(reader, query).size(), 2u);
  auto eraser = caf::scoped_actor{f.sys};
  CHECK_EQUAL(f.erase(eraser, first), caf::error{});
  CHECK_EQUAL(f.erase(eraser, second), caf::error{});
  CHECK(f.files_exist(first));
  CHECK(f.files_exist(second));
  // A reader releases each partition as it finishes with it, so the pin covers
  // one partition read rather than the whole export.
  f.release(reader, query, first);
  CHECK(f.await_deletion(first));
  CHECK(f.files_exist(second));
  f.release(reader, query, second);
  CHECK(f.await_deletion(second));
}

TEST("a deferred erase past its deadline deletes despite a live pin") {
  auto f = fixture{std::chrono::milliseconds{200}};
  const auto id = f.add_partition();
  auto reader = caf::scoped_actor{f.sys};
  const auto query = uuid::random();
  CHECK_EQUAL(fixture::ids_of(f.candidates(reader, query)), std::vector{id});
  auto eraser = caf::scoped_actor{f.sys};
  CHECK_EQUAL(f.erase(eraser, id), caf::error{});
  // The reader never releases. Without the deadline its files would stay on
  // disk forever and the disk budget could never be met.
  CHECK(f.await_deletion(id));
  // The pin it still holds must not double-dispose when it finally goes.
  f.release(reader, query);
  CHECK(not f.files_exist(id));
}

TEST("a zero timeout never forces a deferred erase") {
  auto f = fixture{};
  const auto id = f.add_partition();
  auto reader = caf::scoped_actor{f.sys};
  const auto query = uuid::random();
  CHECK_EQUAL(fixture::ids_of(f.candidates(reader, query)), std::vector{id});
  auto eraser = caf::scoped_actor{f.sys};
  CHECK_EQUAL(f.erase(eraser, id), caf::error{});
  // Nothing forces the deletion, so the files outlive any deadline.
  std::this_thread::sleep_for(300ms);
  CHECK(f.files_exist(id));
  f.release(reader, query);
  CHECK(f.await_deletion(id));
}
