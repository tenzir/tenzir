//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/version.hpp"

#include <caf/make_copy_on_write.hpp>

using namespace tenzir;

namespace {

constexpr auto capacity = size_t{1000};

/// A catalog state populated directly, without an actor. Selection is a pure
/// function of the resident synopses, which is exactly what these tests pin
/// down: what a rebuild would pick, not what running it does.
struct fixture {
  catalog_state state;
  tenzir::time clock = tenzir::time{} + std::chrono::hours{1};

  fixture() {
    factory<synopsis>::initialize();
    state.partition_capacity = capacity;
    state.desired_batch_size = capacity;
  }

  /// Adds a partition and returns its id. Each call advances the clock, so
  /// `max_import_time` orders partitions by insertion.
  /// An unset `approx_bytes` gets a plausible size. Passing zero explicitly
  /// models a partition written before the field existed: its size cannot be
  /// estimated, so it is assumed to fill the budget and never shares a batch.
  auto add(std::string_view schema_name, size_t events,
           uint64_t version = version::current_partition_version,
           Option<uint64_t> size = None{}) -> uuid {
    const auto approx_bytes = size.value_or(events * 100);
    const auto id = uuid::random();
    auto schema
      = type{std::string{schema_name}, record_type{{"msg", string_type{}}}};
    auto synopsis = caf::make_copy_on_write<partition_synopsis>();
    synopsis.unshared().schema = schema;
    synopsis.unshared().events = events;
    synopsis.unshared().version = version;
    synopsis.unshared().approx_bytes = approx_bytes;
    clock += std::chrono::seconds{1};
    synopsis.unshared().min_import_time = clock;
    synopsis.unshared().max_import_time = clock;
    state.update_synopses([&](tenzir::catalog_state::synopsis_map& map) {
      tenzir::catalog_state::mutable_schema(map, schema)[id]
        = std::move(synopsis);
    });
    return id;
  }

  static auto make_run(rebuild_options options) -> rebuild_run {
    auto run = rebuild_run{};
    run.options = std::move(options);
    run.options.expression = trivially_true_expression();
    return run;
  }

  static auto ids_of(const std::vector<partition_info>& batch)
    -> std::vector<uuid> {
    auto result = std::vector<uuid>{};
    for (const auto& partition : batch) {
      result.push_back(partition.uuid);
    }
    return result;
  }
};

/// Rebuilds everything, which is what `rebuild --all` does.
auto all_options() -> rebuild_options {
  return rebuild_options{.all = true, .parallel = 1};
}

/// Rebuilds outdated and undersized partitions, which is the automatic source.
auto automatic_options() -> rebuild_options {
  return rebuild_options{.undersized = true, .parallel = 1, .automatic = true};
}

} // namespace

TEST("two eligible partitions of one schema make a batch") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  const auto second = f.add("test", 10);
  auto run = f.make_run(all_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("a batch never mixes schemas") {
  auto f = fixture{};
  const auto first = f.add("a", 10);
  const auto second = f.add("b", 10);
  const auto third = f.add("a", 10);
  auto run = f.make_run(all_options());
  // Schemas are tried in name order, and "a" has two eligible partitions.
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, third}));
  TENZIR_UNUSED(second);
}

TEST("a batch stops at the partition capacity") {
  auto f = fixture{};
  const auto first = f.add("test", capacity - 1);
  const auto second = f.add("test", capacity - 1);
  f.add("test", capacity - 1);
  auto run = f.make_run(all_options());
  // The first partition already brings the batch within one event of the
  // capacity, so the second closes it out and the third stays behind.
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("selection skips partitions that a transform already holds") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  const auto second = f.add("test", 10);
  const auto third = f.add("test", 10);
  f.state.in_transformation.insert(second);
  auto run = f.make_run(all_options());
  // `apply` would refuse the held partition anyway; selecting it would only
  // waste a slot.
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, third}));
}

TEST("selection picks up partitions merged after the run started") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  auto run = f.make_run(automatic_options());
  // One partition is not a batch yet.
  CHECK(f.state.select_rebuild_batch(run).empty());
  // The automatic source re-selects against live state, so a partition that
  // arrives mid-run joins the next batch. This is not possible with a snapshot
  // taken once per run.
  const auto second = f.add("test", 10);
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("selection skips partitions erased after the run started") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  const auto second = f.add("test", 10);
  const auto third = f.add("test", 10);
  auto run = f.make_run(all_options());
  f.state.erase(second);
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, third}));
}

TEST("a manual run ignores partitions ingested after it started") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  const auto second = f.add("test", 10);
  auto run = f.make_run(all_options());
  // A manual run bounds itself so that it terminates under ongoing ingest.
  run.horizon = f.clock + std::chrono::seconds{1};
  f.add("test", 10);
  f.add("test", 10);
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("the automatic source keeps only outdated and undersized partitions") {
  auto f = fixture{};
  // Current version and comfortably above the undersized threshold.
  f.add("full", capacity);
  f.add("full", capacity);
  const auto small_first = f.add("small", 10);
  const auto small_second = f.add("small", 10);
  auto run = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{small_first, small_second}));
}

TEST("--all rebuilds a partition the automatic source would leave alone") {
  auto f = fixture{};
  // Full, current, and measurable: nothing about it is worth improving, so the
  // automatic source ignores it. Two of them cannot share a batch either,
  // since one already fills the capacity.
  const auto id = f.add("full", capacity);
  f.add("full", capacity);
  auto automatic = f.make_run(automatic_options());
  CHECK(f.state.select_rebuild_batch(automatic).empty());
  // `--all` means "rewrite everything", so it takes them one at a time.
  auto all = f.make_run(all_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(all)),
              std::vector{id});
}

TEST("a run does not select a partition twice") {
  auto f = fixture{};
  const auto first = f.add("test", capacity);
  f.add("test", capacity);
  auto run = f.make_run(all_options());
  const auto batch = f.state.select_rebuild_batch(run);
  CHECK_EQUAL(fixture::ids_of(batch), std::vector{first});
  // Without this a `--all` run would keep reselecting the same partitions --
  // and, once it starts committing, its own output as well.
  for (const auto& partition : batch) {
    run.visited.insert(partition.uuid);
  }
  CHECK_NOT_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
                  std::vector{first});
}

TEST("a run stops selecting at its partition cap") {
  auto f = fixture{};
  f.add("test", 10);
  f.add("test", 10);
  f.add("test", 10);
  f.add("test", 10);
  auto run = f.make_run(all_options());
  run.options.max_partitions = 3;
  CHECK_EQUAL(f.state.select_rebuild_batch(run).size(), 3u);
  // Two of the cap are spent; one partition is left, which is not a batch.
  run.selected = 3;
  CHECK(f.state.select_rebuild_batch(run).empty());
}

TEST("empty partitions are never rebuilt") {
  auto f = fixture{};
  f.add("test", 0);
  f.add("test", 0);
  auto run = f.make_run(all_options());
  CHECK(f.state.select_rebuild_batch(run).empty());
}

TEST("a lone partition whose size is unknown is rebuilt anyway") {
  auto f = fixture{};
  // Partitions written before `approx_bytes` existed are assumed to fill the
  // whole memory budget, so they never share a batch with anything. Refusing
  // lone batches outright would leave them unrebuilt for good -- and they are
  // exactly what a rebuild is for.
  const auto id = f.add("test", 10, version::current_partition_version,
                        /*approx_bytes=*/0);
  auto run = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              std::vector{id});
}

TEST("a lone outdated partition is upgraded without waiting for a partner") {
  auto f = fixture{};
  const auto id = f.add("test", 10, version::current_partition_version - 1);
  auto run = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              std::vector{id});
}

TEST("a lone current undersized partition is left for a batch") {
  auto f = fixture{};
  // Rebuilding it would rewrite it into itself, and it would stay eligible --
  // that is the case that makes continuous re-selection fail to terminate.
  f.add("test", 10);
  auto run = f.make_run(automatic_options());
  CHECK(f.state.select_rebuild_batch(run).empty());
}

TEST("the status reports the quarantine set when no run is in progress") {
  auto f = fixture{};
  // This is what `rebuild show` asks for once a run has finished. Reporting
  // nothing here would leave an operator with no record of a quarantine.
  const auto status = f.state.rebuild_status();
  CHECK(status.find("current-run") == status.end());
  CHECK_EQUAL(status.at("quarantined-size"), data{uint64_t{0}});
}

TEST("quarantined partitions outlive the run that found them") {
  auto f = fixture{};
  const auto id = uuid::random();
  // A quarantine belongs to the catalog, not to the run: the run that found
  // this partition is already gone.
  f.state.quarantined_partitions[id] = "store decode failed";
  const auto status = f.state.rebuild_status();
  CHECK_EQUAL(status.at("quarantined-size"), data{uint64_t{1}});
  const auto* quarantined = try_as<list>(&status.at("quarantined"));
  REQUIRE(quarantined);
  REQUIRE_EQUAL(quarantined->size(), 1u);
  const auto* entry = try_as<record>(&quarantined->front());
  REQUIRE(entry);
  CHECK_EQUAL(entry->at("uuid"), data{fmt::to_string(id)});
  CHECK_EQUAL(entry->at("error"), data{std::string{"store decode failed"}});
}
