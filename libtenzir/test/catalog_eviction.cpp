//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/fixtures/filesystem.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"

#include <caf/make_copy_on_write.hpp>

#include <fstream>

using namespace tenzir;

namespace {

/// A catalog state populated directly, without an actor. Which partitions the
/// budget loop would evict is a pure function of the resident synopses, which
/// is what these tests pin down.
struct fixture {
  catalog_state state;
  tenzir::time clock = tenzir::time{} + std::chrono::hours{1};

  fixture() {
    factory<synopsis>::initialize();
  }

  /// Adds a partition and returns its id. Each call advances the clock, so
  /// partitions are ordered oldest-first by insertion.
  auto add(std::string_view schema_name = "test", uint64_t bytes = 1000)
    -> uuid {
    const auto id = uuid::random();
    auto schema
      = type{std::string{schema_name}, record_type{{"msg", string_type{}}}};
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().schema = schema;
    synopsis.unshared().events = 100;
    synopsis.unshared().approx_bytes = bytes;
    synopsis.unshared().store_file.size = bytes;
    clock += std::chrono::seconds{1};
    synopsis.unshared().min_import_time = clock;
    synopsis.unshared().max_import_time = clock;
    state.catalog_bytes += bytes;
    state.update_synopses([&](tenzir::catalog_state::synopsis_map& map) {
      tenzir::catalog_state::mutable_schema(map, schema)[id]
        = std::move(synopsis);
    });
    return id;
  }

  /// Moves a partition into the deferred set, as an erasure behind a pin does.
  /// `bytes` is split across the partition's files, and `approx_bytes` is set
  /// to something far larger on purpose: the budget loop measures the disk,
  /// so it must not reach for the decoded-size estimate.
  void park(const uuid& id, uint64_t bytes) {
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().store_file.size = bytes / 2;
    synopsis.unshared().indexes_file.size = bytes / 4;
    synopsis.unshared().sketches_file.size = bytes - bytes / 2 - bytes / 4;
    synopsis.unshared().approx_bytes = bytes * 10;
    state.deferred[id] = deferred_erase{.synopsis = std::move(synopsis)};
  }
};

} // namespace

TEST("eviction takes the oldest partitions first") {
  auto f = fixture{};
  const auto first = f.add();
  const auto second = f.add();
  f.add();
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{first, second}));
}

TEST("erasure updates accounting before maintenance scheduling starts") {
  auto f = fixture{};
  const auto removed = f.add("test", 1000);
  f.add("test", 2000);
  // Directly populated synopses model a just-loaded catalog. Initialization
  // accounts for them, but package startup has not enabled scheduling yet.
  f.state.catalog_bytes = 0;
  REQUIRE(not f.state.initialize_maintenance(f.clock).valid());
  REQUIRE(not f.state.maintenance_ready);
  CHECK_EQUAL(f.state.catalog_bytes, uint64_t{3000});
  f.state.erase(removed);
  CHECK_EQUAL(f.state.catalog_bytes, uint64_t{2000});
  f.state.on_space_measured(2000);
  CHECK_EQUAL(f.state.external_bytes, uint64_t{0});
}

TEST("eviction crosses schemas") {
  auto f = fixture{};
  const auto oldest = f.add("a");
  const auto next = f.add("b");
  f.add("a");
  // The budget loop reclaims space, so unlike a rebuild it has no reason to
  // keep a batch within one schema.
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{oldest, next}));
}

TEST("ingest admission does not double count files seen by a directory scan") {
  auto f = fixture{};
  const auto existing = f.add();
  const auto ingested = uuid::random();
  f.state.admissions[existing] = 1;
  // The scan already sees 1000 bytes of unadmitted ingest and 500 bytes of
  // unrelated overhead, in addition to the catalog's existing partition.
  f.state.on_space_measured(2500, {{existing, 1000}, {ingested, 1000}});
  CHECK_EQUAL(f.state.external_bytes, uint64_t{1500});
  CHECK(not f.state.scanned_ingest_bytes.contains(existing));
  auto synopsis = f.state.find_synopsis(existing);
  std::ignore = f.state.merge({{ingested, synopsis}},
                              catalog_state::merge_source::ingest);
  CHECK_EQUAL(f.state.catalog_bytes, uint64_t{2000});
  CHECK_EQUAL(f.state.external_bytes, uint64_t{500});
  // Neither repeated admissions nor replacement outputs take another credit.
  std::ignore = f.state.merge({{ingested, synopsis}},
                              catalog_state::merge_source::ingest);
  CHECK_EQUAL(f.state.catalog_bytes, uint64_t{2000});
  CHECK_EQUAL(f.state.external_bytes, uint64_t{500});
  std::ignore = f.state.merge({{uuid::random(), synopsis}});
  CHECK_EQUAL(f.state.external_bytes, uint64_t{500});
  // Later arrivals were not in the scan and cannot consume real overhead.
  std::ignore = f.state.merge({{uuid::random(), synopsis}},
                              catalog_state::merge_source::ingest);
  CHECK_EQUAL(f.state.external_bytes, uint64_t{500});
  // A partial file measurement credits only the bytes actually observed.
  const auto partial = uuid::random();
  f.state.on_space_measured(f.state.catalog_bytes + 750, {{partial, 250}});
  std::ignore
    = f.state.merge({{partial, synopsis}}, catalog_state::merge_source::ingest);
  CHECK_EQUAL(f.state.external_bytes, uint64_t{500});
  CHECK(f.state.scanned_ingest_bytes.empty());
  // A new accepted scan replaces, rather than accumulates, pending credits.
  f.state.on_space_measured(f.state.catalog_bytes + 1500,
                            {{uuid::random(), 1000}});
  f.state.on_space_measured(f.state.catalog_bytes + 500);
  CHECK(f.state.scanned_ingest_bytes.empty());
}

TEST("directory scans record the partition bytes they actually measured") {
  auto files = fixtures::filesystem{"catalog-eviction-scan"};
  const auto paths = partition_paths::from_database_dir(files.directory);
  std::filesystem::create_directories(paths.index_dir);
  std::filesystem::create_directories(paths.archive_dir);
  const auto id = uuid::random();
  for (const auto& [path, bytes] :
       std::vector<std::pair<std::filesystem::path, size_t>>{
         {paths.partition(id), 10},
         {paths.synopsis(id), 20},
         {paths.archive_dir / fmt::format("{:l}.feather", id), 30},
         {paths.database_dir / "overhead", 50}}) {
    auto out = std::ofstream{path};
    out << std::string(bytes, 'x');
  }
  auto measured = compute_dbdir_size(paths, {});
  REQUIRE(measured);
  CHECK(measured->stable);
  CHECK_EQUAL(measured->bytes, uint64_t{110});
  CHECK_EQUAL(measured->partition_bytes.size(), size_t{1});
  CHECK_EQUAL(measured->partition_bytes.at(id), uint64_t{60});
  auto config = disk_monitor_config{};
  // Use shell builtins so this test needs no external scan program.
  config.scan_binary = "printf 456; #";
  measured = compute_dbdir_size(paths, config);
  REQUIRE(measured);
  CHECK(measured->stable);
  CHECK_EQUAL(measured->bytes, uint64_t{456});
  CHECK_EQUAL(measured->partition_bytes.at(id), uint64_t{60});
  config.scan_binary
    = fmt::format("printf x >> '{}'; printf 456; #", paths.partition(id));
  measured = compute_dbdir_size(paths, config);
  REQUIRE(measured);
  CHECK(not measured->stable);
}

TEST("disk scans wait for catalog commits after their transformer exits") {
  auto f = fixture{};
  CHECK(f.state.space_scan_is_stable());
  const auto id = uuid::random();
  f.state.active_transformations.try_emplace(id);
  REQUIRE(f.state.active_transformers.empty());
  CHECK(not f.state.space_scan_is_stable());
  f.state.active_transformations.erase(id);
  CHECK(f.state.space_scan_is_stable());
  // Failed commits can retain claims and staged files until the next startup,
  // even though neither the transformer nor its continuation is still active.
  f.state.in_transformation.insert(id);
  CHECK(not f.state.space_scan_is_stable());
  f.state.in_transformation.erase(id);
  CHECK(f.state.space_scan_is_stable());
}

TEST("eviction skips a partition a transform is holding") {
  auto f = fixture{};
  const auto first = f.add();
  const auto second = f.add();
  const auto third = f.add();
  f.state.in_transformation.insert(first);
  // Erasing an input would let its data come back through the output.
  CHECK_EQUAL(f.state.select_eviction_batch(2), (std::vector{second, third}));
}

TEST("eviction stops at the step size") {
  auto f = fixture{};
  f.add();
  f.add();
  f.add();
  CHECK_EQUAL(f.state.select_eviction_batch(1).size(), 1u);
  CHECK_EQUAL(f.state.select_eviction_batch(0).size(), 0u);
  // Asking for more than there is yields what there is.
  CHECK_EQUAL(f.state.select_eviction_batch(10).size(), 3u);
}

namespace {

/// A policy that weights one schema above everything else, to show that the
/// catalog orders by what the policy says rather than by age.
struct weighted_policy final : tenzir::storage_policy {
  std::string heavy = {};

  auto eviction_weight(const uuid&, const partition_synopsis& synopsis,
                       tenzir::time now) const -> Option<double> override {
    if (synopsis.schema.name() != heavy) {
      return None{};
    }
    // Scaled age, the way the compaction policy expresses it: a weighted
    // answer stays comparable with the plain age of unweighted partitions.
    const auto age
      = std::chrono::duration<double>{now - synopsis.max_import_time}.count();
    return 1000.0 * age;
  }
};

} // namespace

TEST("a policy weight decides the eviction order") {
  auto f = fixture{};
  // Oldest first, so without a policy this one would go first.
  const auto oldest = f.add("light");
  const auto newest = f.add("heavy");
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{oldest});
  auto policy = std::make_unique<weighted_policy>();
  policy->heavy = "heavy";
  f.state.policy = std::move(policy);
  // The policy outweighs the age difference, so the younger one goes first.
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{newest});
}

TEST("a partition the policy has no weight for keeps its age") {
  auto f = fixture{};
  const auto oldest = f.add("light");
  f.add("light");
  auto policy = std::make_unique<weighted_policy>();
  policy->heavy = "absent";
  f.state.policy = std::move(policy);
  // `none` from the policy falls back to age, which is oldest-first.
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{oldest});
}

TEST("parked bytes count the partitions waiting on their pins") {
  auto f = fixture{};
  CHECK_EQUAL(f.state.parked_bytes(), 0u);
  f.park(uuid::random(), 700);
  f.park(uuid::random(), 300);
  // These are erased already and their files go once the last reader drops, so
  // the budget loop must not erase more data on their account. The count is
  // the on-disk footprint, not the decoded-size estimate.
  CHECK_EQUAL(f.state.parked_bytes(), 1000u);
}

TEST("excluded victims do not shadow the candidates behind them") {
  auto f = fixture{};
  const auto oldest = f.add();
  const auto next = f.add();
  CHECK_EQUAL(f.state.select_eviction_batch(1), std::vector{oldest});
  // A victim the pass could not act on -- full pool, broken pipeline -- must
  // not be re-selected forever while actionable partitions wait behind it.
  CHECK_EQUAL(f.state.select_eviction_batch(1, {oldest}), std::vector{next});
}

TEST("bytes of in-flight deletions count as already reclaimed") {
  auto f = fixture{};
  CHECK_EQUAL(f.state.deleting_bytes(), 0u);
  // Deletion is asynchronous: a database scan can complete before the
  // filesystem actor got to the files. Until it does, the victims' bytes are
  // spoken for, and the budget loop must not select further partitions to
  // cover them.
  f.state.deleting[uuid::random()] = 600;
  f.state.deleting[uuid::random()] = 400;
  CHECK_EQUAL(f.state.deleting_bytes(), 1000u);
}

TEST("a no-progress eviction cannot repeat through its fresh output UUID") {
  auto f = fixture{};
  auto input = uuid::random();
  auto output = f.add("test", 1000);
  auto result = partition_apply_result{};
  result.output_partitions.emplace_back(output, *f.state.find_synopsis(output));
  f.state.record_eviction_result(input, 1000, keep_original_partition::no,
                                 result);
  CHECK(f.state.eviction_suppressed.contains(output));
  CHECK_EQUAL(f.state.run_eviction_action(output), eviction_outcome::deferred);
}

TEST("disk reconciliation separates live files from pending reclamation") {
  auto f = fixture{};
  f.add("test", 1000);
  f.park(uuid::random(), 700);
  f.state.deleting[uuid::random()] = 300;
  f.state.on_space_measured(2500);
  CHECK_EQUAL(f.state.external_bytes, 500u);
  CHECK_EQUAL(f.state.dbdir_size, 2500u);
}

TEST("disk pressure keeps its low-water target across decisions") {
  auto f = fixture{};
  auto id = f.add("test", 2000);
  f.state.in_transformation.insert(id);
  f.state.maintenance.space.high_water_mark = 1500;
  f.state.maintenance.space.low_water_mark = 1000;
  f.state.maintenance.space.scan_interval = std::chrono::seconds{1};
  f.state.enforce_disk_budget();
  CHECK(f.state.evicting);
  f.state.catalog_bytes = 1200;
  f.state.enforce_disk_budget();
  CHECK(f.state.evicting);
  f.state.catalog_bytes = 1000;
  f.state.enforce_disk_budget();
  CHECK(not f.state.evicting);
}

TEST("retiring and pinned files are credited only once") {
  auto f = fixture{};
  auto id = f.add("test", 1000);
  f.state.retiring.insert(id);
  f.state.maintenance.space.high_water_mark = 900;
  f.state.maintenance.space.low_water_mark = 500;
  f.state.maintenance.space.scan_interval = std::chrono::seconds{1};
  f.state.enforce_disk_budget();
  CHECK(not f.state.evicting);
  CHECK_EQUAL(f.state.dbdir_size, 1000u);
  f.state.retiring.clear();
  f.state.catalog_bytes = 0;
  f.park(id, 1000);
  f.state.enforce_disk_budget();
  CHECK(not f.state.evicting);
  CHECK_EQUAL(f.state.dbdir_size, 1000u);
}

TEST("disk eviction enforces known bytes without a scan and respects pause") {
  auto f = fixture{};
  auto id = f.add("test", 2000);
  // Keep selection pure while verifying that unreconciled pressure is active.
  f.state.in_transformation.insert(id);
  f.state.maintenance.space.high_water_mark = 1500;
  f.state.maintenance.space.low_water_mark = 1000;
  f.state.maintenance.space.scan_interval = std::chrono::seconds::zero();
  f.state.enforce_disk_budget();
  CHECK(not f.state.evicting);
  f.state.maintenance.space.scan_interval = std::chrono::seconds{1};
  // No directory measurement is needed, even if one is currently in flight.
  f.state.measuring_space = true;
  f.state.enforce_disk_budget();
  CHECK(f.state.evicting);
  CHECK_EQUAL(f.state.dbdir_size, uint64_t{2000});
}

TEST("eviction waits for rewrite results even with spare disk step allowance") {
  auto f = fixture{};
  f.add("test", 2000);
  f.state.maintenance.space.high_water_mark = 1500;
  f.state.maintenance.space.low_water_mark = 1000;
  f.state.maintenance.space.scan_interval = std::chrono::seconds{1};
  f.state.maintenance.space.step_size = 4;
  f.state.eviction_running = 1;
  // No direct deletion may start until the rewrite's reclamation is known.
  f.state.enforce_disk_budget();
  CHECK(f.state.evicting);
  CHECK(f.state.retiring.empty());
}

TEST("a released slot resumes the shared decision function") {
  auto f = fixture{};
  f.state.maintenance.compaction_slots = 1;
  // A periodic maintenance or eviction action holds the only slot.
  f.state.compacting = 1;
  // Disable real deadlines; this test exercises inline completion only.
  f.state.maintenance_ready = true;
  f.state.next_collection = tenzir::time::max();
  f.state.next_disposal_check = tenzir::time::max();
  auto run = named_rule_run{};
  run.rule = "r";
  run.pending = {uuid::random()};
  // A batch of the run itself is in flight too, so the run is not finished
  // and must not be answered here.
  run.running = 1;
  f.state.named_run.emplace(std::move(run));
  // Nothing moves while the pool is full.
  f.state.drain_named_rule();
  REQUIRE(f.state.named_run);
  CHECK_EQUAL(f.state.named_run->pending.size(), 1u);
  // The slot comes back from that other work, not from this run. If only the
  // run's own completions drained the queue, it would wait here forever and
  // its caller would block on a promise that never lands.
  f.state.release_compaction_slot();
  CHECK_EQUAL(f.state.compacting, 0u);
  REQUIRE(f.state.named_run);
  CHECK(f.state.named_run->pending.empty());
}
