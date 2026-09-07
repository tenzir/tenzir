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
#include "tenzir/plugin/storage_policy.hpp"
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
    state.rebuild_zone = std::chrono::locate_zone("UTC");
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
    state.admissions[id] = ++state.admission_sequence;
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
    state.open_rebuild_groups.emplace(schema, state.rebuild_day(clock));
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
  f.add("test", capacity - 1);
  f.add("test", capacity - 1);
  auto run = f.make_run(all_options());
  // No second input fits under the event cap; --all permits a lone rewrite.
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first}));
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

TEST("new arrivals wait for the next automatic collection") {
  auto f = fixture{};
  const auto first = f.add("test", 10);
  auto run = f.make_run(automatic_options());
  // One partition is not a batch yet.
  CHECK(f.state.select_rebuild_batch(run).empty());
  // An open-hour arrival must not repeatedly re-merge this run's tiny output.
  const auto second = f.add("test", 10);
  CHECK(f.state.select_rebuild_batch(run).empty());
  run = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("a closed collection does not rebuild untouched schema day groups") {
  auto f = fixture{};
  f.add("old", 10);
  f.add("old", 10);
  auto first = f.add("new", 10);
  auto second = f.add("new", 10);
  auto run = f.make_run(automatic_options());
  run.collected_groups = RebuildGroups{
    {f.state.find_synopsis(first)->schema, f.state.rebuild_day(f.clock)}};
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
  run.horizon = f.state.admission_sequence;
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

TEST("rebuild never combines different max import days, including --all") {
  auto f = fixture{};
  f.clock = tenzir::time{} + std::chrono::hours{23};
  auto first = f.add("test", 10);
  f.clock += std::chrono::hours{2};
  auto second = f.add("test", 10);
  auto third = f.add("test", 10);
  auto automatic = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(automatic)),
              (std::vector{second, third}));
  auto all = f.make_run(all_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(all)),
              (std::vector{first}));
}

TEST("day buckets use max import time, not the input's span") {
  auto f = fixture{};
  f.clock = tenzir::time{} + std::chrono::hours{26};
  auto first = f.add("test", 10);
  auto second = f.add("test", 10);
  auto schema = f.state.find_synopsis(first)->schema;
  f.state.update_synopses([&](catalog_state::synopsis_map& map) {
    auto& synopsis = catalog_state::mutable_schema(map, schema)[first];
    synopsis.unshared().min_import_time = tenzir::time{};
  });
  auto run = f.make_run(automatic_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, second}));
}

TEST("a busy compaction pool does not give rebuild its inputs") {
  auto f = fixture{};
  auto first = f.add("test", 10);
  auto held = f.add("test", 10);
  auto third = f.add("test", 10);
  f.state.compacting = f.state.maintenance.compaction_slots;
  f.state.policy_pending.insert(held);
  auto run = f.make_run(all_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first, third}));
  CHECK_EQUAL(run.deferred, 1u);
}

TEST("inline decisions classify due policy before checking pool capacity") {
  struct DuePolicy final : storage_policy {
    uuid input;
    auto maintenance_deadline(uuid const&, partition_synopsis const&,
                              tenzir::time) const -> tenzir::time override {
      return tenzir::time::max();
    }
    auto maintenance_interval() const -> duration override {
      return std::chrono::hours{1};
    }
    auto maintenance_action(uuid const& id, partition_synopsis const&,
                            tenzir::time) const
      -> Option<storage_action> override {
      if (id == input) {
        return storage_action{};
      }
      return None{};
    }
  };
  auto f = fixture{};
  auto id = f.add("test", 10);
  auto policy = std::make_unique<DuePolicy>();
  policy->input = id;
  f.state.policy = std::move(policy);
  f.state.compacting = f.state.maintenance.compaction_slots;
  f.state.maintenance_ready = true;
  f.state.next_collection = tenzir::time::max();
  f.state.next_disposal_check = tenzir::time::max();
  f.state.next_policy_check = tenzir::time::max();
  f.state.policy_dirty.insert(id);
  f.state.advance_maintenance(f.clock);
  CHECK(f.state.policy_dirty.empty());
  CHECK(f.state.policy_pending.contains(id));
  auto run = f.make_run(all_options());
  CHECK(f.state.select_rebuild_batch(run).empty());
  CHECK_EQUAL(run.deferred, 1u);
}

TEST("a retiring input cannot be selected while its marker is being written") {
  auto f = fixture{};
  auto held = f.add("test", 10);
  auto free = f.add("test", 10);
  f.state.retiring.insert(held);
  auto run = f.make_run(all_options());
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{free}));
}

TEST("late arrivals cannot enter a manual run by carrying old timestamps") {
  auto f = fixture{};
  auto first = f.add("test", 10);
  auto run = f.make_run(all_options());
  run.horizon = f.state.admission_sequence;
  f.clock -= std::chrono::minutes{30};
  f.add("test", 10);
  CHECK_EQUAL(fixture::ids_of(f.state.select_rebuild_batch(run)),
              (std::vector{first}));
}

TEST("timezone controls day buckets and preserves both autumn hours") {
  using namespace std::chrono;
  auto f = fixture{};
  f.state.rebuild_zone = locate_zone("Europe/Berlin");
  auto midnight_utc = tenzir::time{sys_days{2026y / October / 25}};
  CHECK_EQUAL(f.state.rebuild_day(midnight_utc - hours{1}),
              floor<days>(midnight_utc.time_since_epoch()).count());
  auto first_hour = midnight_utc + minutes{30};
  auto second_hour = midnight_utc + hours{1} + minutes{30};
  CHECK_EQUAL(f.state.next_rebuild_hour(first_hour), midnight_utc + hours{1});
  CHECK_EQUAL(f.state.next_rebuild_hour(second_hour), midnight_utc + hours{2});
}

TEST("hourly collection skips the nonexistent spring hour") {
  using namespace std::chrono;
  auto f = fixture{};
  f.state.rebuild_zone = locate_zone("Europe/Berlin");
  auto midnight_utc = tenzir::time{sys_days{2026y / March / 29}};
  CHECK_EQUAL(f.state.next_rebuild_hour(midnight_utc + minutes{30}),
              midnight_utc + hours{1});
  CHECK_EQUAL(f.state.next_rebuild_hour(midnight_utc + hours{1} + minutes{30}),
              midnight_utc + hours{2});
}

TEST("invalid explicit rebuild timezone fails before scheduling") {
  auto state = catalog_state{};
  state.maintenance.rebuild_timezone = "Not/A-Timezone";
  CHECK(state.initialize_maintenance(tenzir::time{}).valid());
  CHECK(not state.maintenance_ready);
}

TEST("rebuild defaults to the system timezone and accepts an explicit "
     "override") {
  auto local = catalog_state{};
  CHECK(not local.initialize_maintenance(tenzir::time{}).valid());
  CHECK_EQUAL(local.rebuild_zone->name(), std::chrono::current_zone()->name());
  auto explicit_zone = catalog_state{};
  explicit_zone.maintenance.rebuild_timezone = "UTC";
  CHECK(not explicit_zone.initialize_maintenance(tenzir::time{}).valid());
  CHECK_EQUAL(explicit_zone.rebuild_zone->name(),
              std::chrono::locate_zone("UTC")->name());
}

TEST("arrivals cannot postpone or reopen a collection cutoff") {
  using namespace std::chrono;
  auto f = fixture{};
  f.state.maintenance_ready = true;
  f.state.maintenance.rebuild_interval = hours{1};
  auto boundary = tenzir::time{} + hours{2};
  f.state.next_collection = boundary;
  f.add("test", 10);
  f.state.close_rebuild_collection(boundary - seconds{1});
  CHECK_EQUAL(f.state.next_collection, boundary);
  CHECK(f.state.closed_rebuild_groups.empty());
  f.state.close_rebuild_collection(boundary);
  auto closed = f.state.closed_admission;
  CHECK(not f.state.closed_rebuild_groups.empty());
  f.add("test", 10);
  f.state.close_rebuild_collection(boundary);
  CHECK_EQUAL(f.state.closed_admission, closed);
  CHECK_EQUAL(f.state.next_collection, boundary + hours{1});
  f.state.close_rebuild_collection(boundary - hours{1});
  CHECK_EQUAL(f.state.closed_admission, closed);
  CHECK_EQUAL(f.state.next_collection, boundary + hours{1});
}
