//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/set_operations.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/double_synopsis.hpp"
#include "tenzir/duration_synopsis.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/int64_synopsis.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/plugin/component.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/uint64_synopsis.hpp"

#include <caf/actor_registry.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/expected.hpp>

#include <algorithm>
#include <filesystem>
#include <ranges>
#include <set>
#include <string_view>
#include <unordered_set>

namespace tenzir {

namespace {

TENZIR_ENUM(catalog_slice_selector, fields, schemas, partitions);

/// Collects the ids of every partition in a candidate set.
auto partition_ids(const catalog_lookup_result& candidates)
  -> std::vector<uuid> {
  auto result = std::vector<uuid>{};
  result.reserve(candidates.size());
  for (const auto& [schema, info] : candidates.candidate_infos) {
    for (const auto& partition : info.partition_infos) {
      result.push_back(partition.uuid);
    }
  }
  return result;
}

auto collect_synopses(const catalog_state& state)
  -> std::vector<partition_synopsis_pair> {
  auto result = std::vector<partition_synopsis_pair>{};
  result.reserve(state.synopses_per_type->size());
  for (const auto& [schema, id_synopsis_map] : *state.synopses_per_type) {
    for (const auto& [id, synopsis] : *id_synopsis_map) {
      result.push_back({id, synopsis});
    }
  }
  return result;
}

auto collect_synopses(catalog_state& state, const expression& filter)
  -> caf::expected<std::vector<partition_synopsis_pair>> {
  auto result = std::vector<partition_synopsis_pair>{};
  const auto candidates = state.lookup(filter);
  if (not candidates) {
    return candidates.error();
  }
  for (const auto& [schema, candidate] : candidates->candidate_infos) {
    const auto& partition_synopses = state.synopses_per_type->find(schema);
    TENZIR_ASSERT(partition_synopses != state.synopses_per_type->end());
    for (const auto& partition : candidate.partition_infos) {
      const auto& synopsis = partition_synopses->second->find(partition.uuid);
      if (synopsis == partition_synopses->second->end()) {
        continue;
      }
      result.push_back({synopsis->first, synopsis->second});
    }
  }
  return result;
}

auto field_type() -> type {
  return type{
    "tenzir.field",
    record_type{
      {"schema", string_type{}},
      {"schema_id", string_type{}},
      {"field", string_type{}},
      {"path", list_type{string_type{}}},
      {"index", list_type{uint64_type{}}},
      {"type",
       record_type{
         {"kind", string_type{}},
         {"category", string_type{}},
         {"lists", uint64_type()},
         {"name", string_type{}},
         {"attributes", list_type{record_type{
                          {"key", string_type{}},
                          {"value", string_type{}},
                        }}},
       }},
    },
  };
}

struct field_context {
  std::string name{};
  std::vector<std::string> path{};
  offset index{};
};

struct type_context {
  type_kind kind{};
  std::string category;
  size_t lists{0};
  std::string name{};
  std::vector<std::pair<std::string, std::string>> attributes{};
};

struct schema_context {
  field_context field;
  type_context type;
};

auto traverse(type t) -> generator<schema_context> {
  schema_context result;
  while (const auto* list = try_as<list_type>(&t)) {
    ++result.type.lists;
    t = list->value_type();
  }
  result.type.name = t.name();
  for (auto [key, value] : t.attributes()) {
    result.type.attributes.emplace_back(key, value);
  }
  result.type.kind = t.kind();
  if (result.type.kind.is<record_type>()) {
    result.type.category = "container";
  } else {
    result.type.category = "atomic";
  }
  TENZIR_ASSERT(not is<list_type>(t));
  TENZIR_ASSERT(not is<map_type>(t));
  if (const auto* record = try_as<record_type>(&t)) {
    auto i = size_t{0};
    for (const auto& field : record->fields()) {
      result.field.name = field.name;
      result.field.path.emplace_back(field.name);
      result.field.index.emplace_back(i);
      for (const auto& inner : traverse(field.type)) {
        result.type = inner.type;
        auto nested = not inner.field.name.empty();
        if (nested) {
          result.field.name = inner.field.name;
          for (const auto& path : inner.field.path) {
            result.field.path.push_back(path);
          }
          for (const auto& index : inner.field.index) {
            result.field.index.push_back(index);
          }
        }
        co_yield result;
        if (nested) {
          auto delta = inner.field.path.size();
          result.field.path.resize(result.field.path.size() - delta);
          delta = inner.field.index.size();
          result.field.index.resize(result.field.index.size() - delta);
        }
      }
      result.field.index.pop_back();
      result.field.path.pop_back();
      ++i;
    }
  } else {
    co_yield result;
  }
}

auto add_field(builder_ref builder, const type& t) -> void {
  for (const auto& ctx : traverse(t)) {
    auto row = builder.record();
    row.field("schema").data(t.name());
    row.field("schema_id").data(t.make_fingerprint());
    row.field("field").data(ctx.field.name);
    auto path = row.field("path").list();
    for (const auto& part : ctx.field.path) {
      path.data(part);
    }
    auto index = row.field("index").list();
    for (auto i : ctx.field.index) {
      index.data(uint64_t{i});
    }
    auto type = row.field("type").record();
    type.field("kind").data(to_string(ctx.type.kind));
    type.field("category").data(ctx.type.category);
    type.field("lists").data(ctx.type.lists);
    type.field("name").data(ctx.type.name);
    auto attrs = type.field("attributes").list();
    for (const auto& [key, value] : ctx.type.attributes) {
      auto attr = attrs.record();
      attr.field("key").data(key);
      attr.field("value").data(value);
    }
  }
}

auto build_field_slices(const std::vector<partition_synopsis_pair>& synopses)
  -> std::vector<table_slice> {
  auto fields = std::set<type>{};
  for (const auto& synopsis : synopses) {
    fields.insert(synopsis.synopsis->schema);
  }
  auto builder = series_builder{field_type()};
  for (const auto& schema : fields) {
    add_field(builder, schema);
  }
  return builder.finish_as_table_slice();
}

auto build_schema_slices(const std::vector<partition_synopsis_pair>& synopses)
  -> std::vector<table_slice> {
  auto schemas = std::unordered_set<type>{};
  for (const auto& [id, synopsis] : synopses) {
    TENZIR_UNUSED(id);
    TENZIR_ASSERT(synopsis);
    TENZIR_ASSERT(synopsis->schema);
    schemas.insert(synopsis->schema);
  }
  auto builder = series_builder{};
  auto result = std::vector<table_slice>{};
  result.reserve(schemas.size());
  for (const auto& schema : schemas) {
    builder.data(schema.to_definition());
    result.push_back(builder.finish_assert_one_slice(
      fmt::format("tenzir.schema.{}", schema.make_fingerprint())));
  }
  return result;
}

auto build_partition_slices(const std::vector<partition_synopsis_pair>& synopses)
  -> std::vector<table_slice> {
  auto builder = series_builder{};
  for (const auto& synopsis : synopses) {
    auto event = builder.record();
    event.field("uuid").data(fmt::to_string(synopsis.uuid));
    event.field("memusage").data(synopsis.synopsis->memusage());
    event.field("diskusage")
      .data(synopsis.synopsis->store_file.size
            + synopsis.synopsis->indexes_file.size
            + synopsis.synopsis->sketches_file.size);
    event.field("events").data(synopsis.synopsis->events);
    event.field("approx_bytes").data(synopsis.synopsis->approx_bytes);
    event.field("min_import_time").data(synopsis.synopsis->min_import_time);
    event.field("max_import_time").data(synopsis.synopsis->max_import_time);
    event.field("version").data(synopsis.synopsis->version);
    event.field("schema").data(synopsis.synopsis->schema.name());
    event.field("schema_id").data(synopsis.synopsis->schema.make_fingerprint());
    event.field("internal")
      .data(synopsis.synopsis->schema.attribute("internal").has_value());
    auto add_resource = [&](std::string_view key, const resource& value) {
      auto x = event.field(key).record();
      x.field("url").data(value.url);
      x.field("size").data(value.size);
    };
    add_resource("store", synopsis.synopsis->store_file);
    add_resource("indexes", synopsis.synopsis->indexes_file);
    add_resource("sketches", synopsis.synopsis->sketches_file);
  }
  return builder.finish_as_table_slice("tenzir.partition");
}

auto build_catalog_slices(catalog_slice_selector selector,
                          const std::vector<partition_synopsis_pair>& synopses)
  -> std::vector<table_slice> {
  switch (selector) {
    case catalog_slice_selector::fields:
      return build_field_slices(synopses);
    case catalog_slice_selector::schemas:
      return build_schema_slices(synopses);
    case catalog_slice_selector::partitions:
      return build_partition_slices(synopses);
  }
  TENZIR_UNREACHABLE();
}

} // namespace

auto catalog_lookup_result::size() const noexcept -> size_t {
  return std::accumulate(candidate_infos.begin(), candidate_infos.end(),
                         size_t{0}, [](auto i, const auto& cat_result) {
                           return std::move(i)
                                  + cat_result.second.partition_infos.size();
                         });
}

auto catalog_lookup_result::empty() const noexcept -> bool {
  return candidate_infos.empty();
}

auto sketch_cache::peek(const uuid& id) const -> partition_synopsis_ptr {
  const auto it = entries_.find(id);
  if (it == entries_.end()) {
    return nullptr;
  }
  return it->second.synopsis;
}

auto sketch_cache::get(const uuid& id) -> partition_synopsis_ptr {
  const auto it = entries_.find(id);
  if (it == entries_.end()) {
    return nullptr;
  }
  lru_.splice(lru_.begin(), lru_, it->second.pos);
  return it->second.synopsis;
}

auto sketch_cache::put(const uuid& id, partition_synopsis_ptr synopsis)
  -> size_t {
  if (budget_ == 0 or not synopsis) {
    return 0;
  }
  erase(id);
  const auto bytes = synopsis->memusage();
  // Never cache an entry that alone exceeds the budget; keeping it would
  // violate the configured memory cap. The partition simply stays a
  // conservative candidate. Returning zero also keeps the caller from
  // spending its query budget on a sketch that wasn't cached.
  if (bytes > budget_) {
    return 0;
  }
  lru_.push_front(id);
  used_ += bytes;
  entries_.emplace(id, entry{std::move(synopsis), bytes, lru_.begin()});
  // Evict least-recently-used entries until within budget. The entry we just
  // inserted fits (checked above) and is most-recently-used, so eviction only
  // ever removes older entries.
  while (used_ > budget_) {
    const auto victim = lru_.back();
    erase(victim);
  }
  return bytes;
}

void sketch_cache::erase(const uuid& id) {
  const auto it = entries_.find(id);
  if (it == entries_.end()) {
    return;
  }
  used_ -= it->second.bytes;
  lru_.erase(it->second.pos);
  entries_.erase(it);
}

auto catalog_state::initialize(std::vector<partition_synopsis_pair> partitions)
  -> caf::error {
  auto unsupported_partitions = std::vector<uuid>{};
  for (const auto& [uuid, synopsis] : partitions) {
    auto supported = version::support_for_partition_version(synopsis->version);
    if (supported.end_of_life) {
      unsupported_partitions.push_back(uuid);
    }
  }
  if (not unsupported_partitions.empty()) {
    return caf::make_error(
      ec::version_error,
      fmt::format("{} cannot load unsupported partitions; please run "
                  "'tenzir-ctl rebuild' with at least {} to rebuild the "
                  "following partitions, or delete them from the database "
                  "directory: {}",
                  *self,
                  version::support_for_partition_version(
                    version::current_partition_version)
                    .introduced,
                  fmt::join(unsupported_partitions, ", ")));
  }
  using flat_data_list = std::vector<std::pair<uuid, partition_synopsis_ptr>>;
  auto flat_data_map = std::unordered_map<tenzir::type, flat_data_list>{};
  for (auto& [uuid, synopsis] : partitions) {
    TENZIR_ASSERT(synopsis->get_reference_count() == 1ull);
    flat_data_map[synopsis->schema].emplace_back(uuid, std::move(synopsis));
  }
  update_synopses([&](synopsis_map& map) {
    for (auto& [type, flat_data] : flat_data_map) {
      std::ranges::sort(flat_data, std::ranges::less{},
                        &flat_data_list::value_type::first);
      map[type] = std::make_shared<const schema_synopsis_map>(
        schema_synopsis_map::make_unsafe(std::move(flat_data)));
    }
  });
  return caf::none;
}

auto catalog_state::merge(std::vector<partition_synopsis_pair> partitions,
                          merge_source source) -> caf::result<atom::ok> {
  if (partitions.empty()) {
    return atom::ok_v;
  }
  // Close the previous hour before admitting either ingested partitions or
  // replacement outputs, so both wait for the next automatic collection.
  close_rebuild_collection(time::clock::now());
  update_synopses([&](synopsis_map& map) {
    // Clone each touched schema exactly once for the whole batch.
    auto cloned = std::unordered_map<type, schema_synopsis_map*>{};
    for (auto& [id, synopsis] : partitions) {
      const auto footprint = synopsis->store_file.size
                             + synopsis->indexes_file.size
                             + synopsis->sketches_file.size;
      if (auto old = find_synopsis(id)) {
        catalog_bytes -= old->store_file.size + old->indexes_file.size
                         + old->sketches_file.size;
      } else if (source == merge_source::ingest) {
        // Credit only this partition's bytes observed by the accepted scan.
        // Later arrivals must not consume unrelated non-partition overhead.
        if (auto observed = scanned_ingest_bytes.find(id);
            observed != scanned_ingest_bytes.end()) {
          external_bytes
            -= std::min({external_bytes, footprint, observed->second});
          scanned_ingest_bytes.erase(observed);
        }
      }
      catalog_bytes += footprint;
      admissions[id] = ++admission_sequence;
      open_rebuild_groups.emplace(synopsis->schema,
                                  rebuild_day(synopsis->max_import_time));
      ++storage_generation;
      policy_dirty.insert(id);
      // With lazy sketches, drop the Bloom filters of newly flushed or
      // transformed partitions too; otherwise ongoing ingest would accumulate
      // them in resident memory and bypass the bounded sketch cache. They are
      // reloaded on demand from the partition's `.mdx`, so only defer when
      // that file is locally loadable -- never strip sketches we could not
      // reload.
      if (lazy_sketches and synopsis
          and synopsis->sketches_file.url.starts_with("file://")) {
        synopsis.unshared().defer_bloom_filters();
      }
      auto [it, inserted] = cloned.try_emplace(synopsis->schema, nullptr);
      if (inserted) {
        it->second = &mutable_schema(map, synopsis->schema);
      }
      (*it->second)[id] = std::move(synopsis);
      // Drop any stale loaded sketches for a replaced partition.
      invalidate_sketches(id);
    }
  });
  return atom::ok_v;
}

auto catalog_state::mutable_schema(synopsis_map& map, const type& schema)
  -> schema_synopsis_map& {
  auto copy = std::shared_ptr<schema_synopsis_map>{};
  if (const auto it = map.find(schema); it != map.end() and it->second) {
    copy = std::make_shared<schema_synopsis_map>(*it->second);
  } else {
    copy = std::make_shared<schema_synopsis_map>();
  }
  auto& result = *copy;
  map[schema] = std::move(copy);
  return result;
}

void catalog_state::erase(const uuid& partition, notify_policy notify) {
  // Admissions identify accounted partitions, including while package startup
  // still holds back maintenance scheduling.
  if (auto synopsis = find_synopsis(partition);
      synopsis and admissions.contains(partition)) {
    catalog_bytes -= synopsis->store_file.size + synopsis->indexes_file.size
                     + synopsis->sketches_file.size;
  }
  ++storage_generation;
  admissions.erase(partition);
  policy_dirty.erase(partition);
  policy_pending.erase(partition);
  eviction_suppressed.erase(partition);
  if (policy and notify == notify_policy::yes) {
    policy->on_erased(partition);
  }
  invalidate_sketches(partition);
  // Find the containing schema first, so a miss clones nothing.
  const auto containing = std::invoke([&]() -> Option<type> {
    for (const auto& [schema, entries] : *synopses_per_type) {
      if (entries->find(partition) != entries->end()) {
        return schema;
      }
    }
    return None{};
  });
  if (not containing) {
    return;
  }
  update_synopses([&](synopsis_map& map) {
    auto& entries = mutable_schema(map, *containing);
    entries.erase(partition);
    if (entries.empty()) {
      map.erase(*containing);
    }
  });
}

auto catalog_state::find_synopsis(const uuid& partition) const
  -> partition_synopsis_ptr {
  for (const auto& [schema, entries] : *synopses_per_type) {
    if (const auto it = entries->find(partition); it != entries->end()) {
      return it->second;
    }
  }
  return {};
}

auto catalog_state::add_lease(const uuid& query,
                              const caf::strong_actor_ptr& owner,
                              std::vector<uuid> partitions) -> uint64_t {
  // A query without an id cannot be released by its owner, and an anonymous
  // sender cannot be monitored; either way we would hold the lease forever.
  if (partitions.empty() or query == uuid{} or not owner) {
    return 0;
  }
  const auto owner_addr = owner->address();
  for (const auto& partition : partitions) {
    ++pin_counts[partition];
  }
  // A second lookup under the same query id supersedes the first one; that
  // should not happen, but leaking the old lease would pin its partitions
  // forever.
  release_lease(query);
  const auto generation = ++lease_generation;
  leases.emplace(query, partition_lease{
                          .owner = owner_addr,
                          .partitions = std::move(partitions),
                          .generation = generation,
                        });
  auto [consumer, inserted] = consumers.try_emplace(owner_addr);
  if (inserted) {
    consumer->second.second
      = self->monitor(caf::actor_cast<caf::actor>(owner),
                      [this, owner_addr](const caf::error&) {
                        release_leases_of(owner_addr);
                      });
  }
  ++consumer->second.first;
  return generation;
}

void catalog_state::narrow_lease(const uuid& query, uint64_t generation,
                                 const std::vector<uuid>& keep) {
  const auto it = leases.find(query);
  if (it == leases.end() or it->second.generation != generation) {
    // The lease is gone or superseded; whoever owns the pins now decides.
    return;
  }
  if (keep.empty()) {
    release_lease(query);
    return;
  }
  // `keep` is a subset of the leased partitions by construction: candidates
  // come from the very snapshot the provisional lease covered.
  const auto keep_set = std::unordered_set<uuid>{keep.begin(), keep.end()};
  for (const auto& partition : it->second.partitions) {
    if (not keep_set.contains(partition)) {
      unpin(partition);
    }
  }
  it->second.partitions = keep;
}

void catalog_state::release_lease(const uuid& query,
                                  const std::vector<uuid>& partitions) {
  const auto it = leases.find(query);
  if (it == leases.end()) {
    return;
  }
  for (const auto& partition : partitions) {
    // A partition this lease does not hold is not an error: a reader that
    // retries a query may release the same one twice.
    if (std::erase(it->second.partitions, partition) == 0) {
      continue;
    }
    unpin(partition);
  }
  if (it->second.partitions.empty()) {
    release_lease(query);
  }
}

void catalog_state::release_lease(const uuid& query) {
  const auto it = leases.find(query);
  if (it == leases.end()) {
    return;
  }
  const auto lease = std::move(it->second);
  leases.erase(it);
  for (const auto& partition : lease.partitions) {
    unpin(partition);
  }
  const auto consumer = consumers.find(lease.owner);
  if (consumer == consumers.end()) {
    return;
  }
  if (--consumer->second.first == 0) {
    consumer->second.second.dispose();
    consumers.erase(consumer);
  }
}

void catalog_state::release_leases_of(const caf::actor_addr& owner) {
  auto queries = std::vector<uuid>{};
  for (const auto& [query, lease] : leases) {
    if (lease.owner == owner) {
      queries.push_back(query);
    }
  }
  for (const auto& query : queries) {
    release_lease(query);
  }
}

auto catalog_state::deferred_erase_deadline() const -> Option<time> {
  if (deferred_erase_timeout == duration::zero()) {
    return None{};
  }
  return time::clock::now() + deferred_erase_timeout;
}

void catalog_state::sweep_deferred() {
  const auto now = time::clock::now();
  // Retry failed disposals first. They are unpinned -- a disposal only starts
  // once the pins are gone, and a partition that left the catalog gains no
  // new ones -- and their retry has nothing to do with the forcing timeout.
  auto retries = std::vector<uuid>{};
  for (const auto& [partition, erasure] : deferred) {
    if (erasure.retry_at and *erasure.retry_at <= now
        and not pin_counts.contains(partition)) {
      retries.push_back(partition);
    }
  }
  for (const auto& partition : retries) {
    const auto entry = deferred.find(partition);
    auto erasure = std::move(entry->second);
    deferred.erase(entry);
    dispose_of(partition, std::move(erasure), None{});
  }
  auto expired = std::vector<uuid>{};
  for (const auto& [partition, erasure] : deferred) {
    if (erasure.deadline and *erasure.deadline <= now) {
      expired.push_back(partition);
    }
  }
  for (const auto& partition : expired) {
    const auto entry = deferred.find(partition);
    auto erasure = std::move(entry->second);
    deferred.erase(entry);
    // Name who is holding it: a count says something is stuck, the owner says
    // what. Scanning the leases is fine here -- this only runs when a deletion
    // is actually being forced.
    auto holders = std::vector<std::string>{};
    for (const auto& [id, lease] : leases) {
      if (std::ranges::find(lease.partitions, partition)
          != lease.partitions.end()) {
        holders.push_back(fmt::to_string(lease.owner));
      }
    }
    std::ranges::sort(holders);
    holders.erase(std::unique(holders.begin(), holders.end()), holders.end());
    // The holder keeps its pin; it simply finds the files gone. That is the
    // trade the timeout makes, and it is loud on purpose.
    TENZIR_WARN("{} deletes partition {} after waiting {} for {} to release "
                "it; a reader that has not opened it yet will read short",
                *self, partition, data{deferred_erase_timeout},
                holders.empty()
                  ? std::string{"a retriever it can no longer name"}
                  : fmt::format("{}", fmt::join(holders, ", ")));
    dispose_of(partition, std::move(erasure), None{});
  }
}

auto catalog_state::next_lookup_worker() -> const catalog_lookup_worker_actor& {
  TENZIR_ASSERT(not lookup_pool.empty());
  const auto index = next_lookup_worker_index;
  next_lookup_worker_index
    = (next_lookup_worker_index + 1) % lookup_pool.size();
  return lookup_pool[index];
}

void catalog_state::invalidate_sketches(const uuid& partition) {
  for (const auto& worker : lookup_pool) {
    self->mail(atom::erase_v, partition).send(worker);
  }
}

void catalog_state::unpin(const uuid& partition) {
  const auto it = pin_counts.find(partition);
  TENZIR_ASSERT(it != pin_counts.end());
  TENZIR_ASSERT(it->second > 0);
  if (--it->second > 0) {
    return;
  }
  pin_counts.erase(it);
  const auto entry = deferred.find(partition);
  if (entry == deferred.end()) {
    return;
  }
  auto erasure = std::move(entry->second);
  deferred.erase(entry);
  TENZIR_DEBUG("{} disposes of partition {} after the last pin went away",
               *self, partition);
  dispose_of(partition, std::move(erasure), None{});
  advance_maintenance(time::clock::now());
}

namespace {

/// Recovers the filesystem path from a `resource`'s `url`, which is always
/// written as a literal `file://` prefix followed by the canonical path (see
/// e.g. the catalog's population of `partition_synopsis::store_file`), never a
/// percent-encoded or otherwise escaped URI.
auto path_from_file_url(const resource& res) -> std::filesystem::path {
  constexpr auto prefix = std::string_view{"file://"};
  if (res.url.starts_with(prefix)) {
    return std::filesystem::path{res.url.substr(prefix.size())};
  }
  return std::filesystem::path{res.url};
}

} // namespace

auto catalog_state::marker_referenced(const std::filesystem::path& marker) const
  -> bool {
  if (markers_in_disposal.contains(marker)) {
    return true;
  }
  return std::ranges::any_of(deferred, [&](const auto& entry) {
    return entry.second.marker == marker;
  });
}

void catalog_state::erase_marker_if_unreferenced(
  const std::filesystem::path& marker) {
  if (marker.empty()) {
    return;
  }
  if (marker_referenced(marker)) {
    return;
  }
  // Erase errors don't matter too much here: a leftover marker is replayed at
  // the next startup, which erases partitions that are already gone.
  self->mail(atom::erase_v, marker)
    .request(filesystem, caf::infinite)
    .then([](atom::done) { /* nop */ },
          [self = self, marker](const caf::error& err) {
            TENZIR_DEBUG("{} failed to erase marker at {}: {}", *self, marker,
                         err);
          });
}

void catalog_state::release_marker_hold(const std::filesystem::path& marker) {
  const auto it = markers_in_disposal.find(marker);
  TENZIR_ASSERT(it != markers_in_disposal.end());
  if (--it->second == 0) {
    markers_in_disposal.erase(it);
  }
  erase_marker_if_unreferenced(marker);
}

auto catalog_state::invalidate_policy_history() -> caf::error {
  return tenzir::invalidate_policy_history(paths.database_dir);
}

void catalog_state::release_marker_after_flush(std::filesystem::path marker) {
  markers_waiting_for_flush.push_back(std::move(marker));
  if (next_policy_flush == time::max()) {
    next_policy_flush = time::clock::now() + std::chrono::seconds{10};
  }
  if (self and maintenance_ready) {
    arm_maintenance_wakeup(time::clock::now());
  }
}

auto catalog_state::flush_policy_markers() -> caf::error {
  auto error = policy ? policy->flush()
               : markers_waiting_for_flush.empty()
                 ? caf::error{}
                 : invalidate_policy_history();
  if (error.valid()) {
    if (not policy) {
      TENZIR_WARN("{} retains replacement lineage because policy history "
                  "could not be invalidated: {}",
                  name, error);
    }
    // One retry covers the entire batch, including commits arriving meanwhile.
    next_policy_flush = time::clock::now() + defaults::disposal_retry_delay;
    return error;
  }
  next_policy_flush = time::max();
  for (const auto& marker : std::exchange(markers_waiting_for_flush, {})) {
    release_marker_hold(marker);
  }
  return {};
}

void catalog_state::retire_erased(const uuid& partition,
                                  partition_synopsis_ptr synopsis,
                                  std::filesystem::path marker) {
  auto erasure = deferred_erase{
    .synopsis = std::move(synopsis),
    .marker = std::move(marker),
    .quarantine_error = None{},
    .deadline = deferred_erase_deadline(),
  };
  if (pin_counts.contains(partition)) {
    TENZIR_DEBUG("{} defers the deletion of partition {} because a retriever "
                 "still holds it",
                 *self, partition);
    deferred.emplace(partition, std::move(erasure));
    return;
  }
  dispose_of(partition, std::move(erasure), None{});
}

catalog_state::catalog_state() = default;

catalog_state::~catalog_state() {
  maintenance_wakeup.dispose();
}

auto catalog_state::make_policy() -> caf::error {
  for (const auto* plugin : plugins::get<storage_policy_plugin>()) {
    auto candidate = plugin->make_storage_policy(storage_policy_context{
      // The policy has no actor context of its own, so it borrows the
      // catalog's. Both callbacks answer on the catalog's thread, and the
      // filesystem actor resolves the paths against the database directory.
      .read = [dbdir = paths.database_dir](
                std::filesystem::path path) -> caf::expected<chunk_ptr> {
        // Blocking, and deliberately not the filesystem actor: this runs
        // once, inside make_policy() during the catalog's startup, next to
        // the equally blocking marker replay and synopsis scan. A missing
        // file is the ordinary first-start case. Any other failure means
        // the file is there but unreadable, and the policy must hear about
        // the difference: mistaking a transient read error for a clean
        // slate would hand it an empty state to act on.
        const auto resolved = dbdir / path;
        auto err = std::error_code{};
        const auto present = std::filesystem::exists(resolved, err);
        if (err) {
          return caf::make_error(ec::filesystem_error,
                                 fmt::format("failed to probe {}: {}", resolved,
                                             err.message()));
        }
        if (not present) {
          return chunk_ptr{};
        }
        auto chunk = chunk::mmap(resolved);
        if (not chunk) {
          return std::move(chunk.error());
        }
        return std::move(*chunk);
      },
      .write = [dbdir = paths.database_dir](std::filesystem::path path,
                                            chunk_ptr chunk) -> caf::error {
        // Blocking, mirroring the filesystem actor's own write handler.
        if (not chunk) {
          return caf::make_error(ec::invalid_argument,
                                 fmt::format("cannot write a nullptr to {}",
                                             path));
        }
        const auto resolved = path.is_absolute() ? path : dbdir / path;
        return io::save(resolved, as_bytes(chunk));
      },
      .run_delayed =
        [self = this->self](duration delay, std::function<void()> what) {
          detail::weak_run_delayed(self, delay, std::move(what));
        },
    });
    if (not candidate) {
      // An unconfigured implementation declines, which is not an error.
      continue;
    }
    TENZIR_INFO("{} takes its storage policy from the {} plugin", *self,
                plugin->name());
    policy = std::move(candidate);
    // One policy: a second would have to be reconciled with the first on every
    // question, and there is no sensible way to do that.
    break;
  }
  return replay_policy_transforms();
}

auto catalog_state::replay_policy_transforms() -> caf::error {
  auto held_markers = std::vector<std::filesystem::path>{};
  for (const auto& replayed : replayed_transforms) {
    if (not replayed.marker.empty()) {
      if (not policy and replayed.erasure) {
        // An erasure creates no replacement lineage. Its independent file
        // disposal references still keep the tombstone alive as needed.
        release_marker_hold(replayed.marker);
      } else {
        held_markers.push_back(replayed.marker);
      }
    }
  }
  if (not policy) {
    // One durable invalidation replaces arbitrarily many lineage markers.
    // A subsequently enabled policy must not trust history predating it.
    for (auto const& marker : held_markers) {
      release_marker_after_flush(marker);
    }
    static_cast<void>(flush_policy_markers());
    replayed_transforms.clear();
    return {};
  }
  // Validate the whole replay before changing history. Otherwise an unknown
  // token could become a generic replacement, lose its rule watermark, and
  // let a non-idempotent rule run again. Keep every marker and refuse startup
  // until the recorded commit can be recovered.
  for (auto const& replayed : replayed_transforms) {
    if (replayed.policy_token.empty() and not replayed.token_input) {
      continue;
    }
    if (replayed.policy_token.empty() or not replayed.token_input
        or not policy->deserialize_token(replayed.policy_token).has_value()) {
      return caf::make_error(
        ec::format_error,
        fmt::format("cannot replay storage policy token in {}; the marker "
                    "is retained and must be repaired before restarting",
                    replayed.marker));
    }
  }
  // Transforms whose markers replayed at startup finished without their
  // policy callbacks -- the crash landed between the durable marker and the
  // continuation that would have called them. Feeding them now lets the
  // history follow the data to its current ids and lands the interrupted
  // commit; the policy's construction settled its state, so these land in a
  // live history (or the degraded journal), and state the history already
  // saw replays as a no-op.
  //
  // Chained transforms must feed in lineage order: with `A -> B` and
  // `B -> C` pending, replaying `B -> C` first would find no state on `B` to
  // carry, and the records `A -> B` restores afterwards would sit on the
  // vanished `B` forever. Directory iteration guarantees no order, so pick,
  // each round, a transform whose inputs no other pending transform
  // produces.
  auto pending = std::exchange(replayed_transforms, {});
  auto ordered = std::vector<replayed_transform>{};
  ordered.reserve(pending.size());
  while (not pending.empty()) {
    // A transform writes history onto its outputs -- and, for a
    // preserve-input commit, onto its token_input, the surviving partition a
    // later transform may consume. A candidate is ready when no *other*
    // pending transform still writes onto an input, including token_input.
    // Only a preserving transform writes back onto its token_input: treating
    // a consumer as a writer would create a false dependency cycle.
    const auto writes_onto = [](const replayed_transform& transform,
                                const uuid& id) {
      if (transform.inputs.empty() and transform.token_input
          and *transform.token_input == id) {
        return true;
      }
      return std::ranges::any_of(transform.outputs, [&](const auto& output) {
        return output.uuid == id;
      });
    };
    auto chosen = std::ranges::find_if(pending, [&](const auto& transform) {
      return std::ranges::none_of(pending, [&](const auto& other) {
        if (&other == &transform) {
          return false;
        }
        // Two preserving commits both write back onto the same input. The
        // later one depends on the earlier one, not vice versa. Equal (legacy
        // zero) sequences remain ambiguous and fail below rather than guessing.
        if (transform.inputs.empty() and other.inputs.empty()
            and transform.token_input
            and transform.token_input == other.token_input
            and other.sequence > transform.sequence) {
          return false;
        }
        return (transform.token_input
                and writes_onto(other, *transform.token_input))
               or std::ranges::any_of(transform.inputs, [&](const auto& input) {
                    return writes_onto(other, input);
                  });
      });
    });
    if (chosen == pending.end()) {
      return caf::make_error(
        ec::format_error,
        "cannot order transform marker replay; ambiguous or cyclic markers "
        "are retained and must be repaired before restarting");
    }
    ordered.push_back(std::move(*chosen));
    pending.erase(chosen);
  }
  for (auto& replayed : ordered) {
    if (replayed.erasure) {
      for (const auto& input : replayed.inputs) {
        policy->on_erased(input);
      }
    } else if (not replayed.inputs.empty()) {
      policy->on_replaced(replayed.inputs, replayed.outputs);
    }
    if (not replayed.policy_token.empty() and replayed.token_input) {
      policy->on_committed(policy->deserialize_token(replayed.policy_token),
                           *replayed.token_input, replayed.outputs);
    }
  }
  // The replay kept token-carrying markers alive; they may go only once the
  // fed state is durable.
  if (not held_markers.empty()) {
    for (const auto& marker : held_markers) {
      release_marker_after_flush(marker);
    }
    static_cast<void>(flush_policy_markers());
  }
  return {};
}

auto catalog_state::retire(const uuid& partition,
                           Option<std::string> quarantine_error)
  -> caf::result<atom::done> {
  retiring.insert(partition);
  auto erasure = deferred_erase{
    .synopsis = find_synopsis(partition),
    .marker = {},
    .quarantine_error = std::move(quarantine_error),
    .deadline = deferred_erase_deadline(),
  };
  auto rp = self->make_response_promise<atom::done>();
  // Runs once the retirement is safe to act on, with the world re-checked:
  // the partition stays live while a tombstone write is in flight, so a
  // concurrent retirement, a transform, or a vanished pin may have beaten us.
  auto proceed = [this, partition, rp](deferred_erase erasure) mutable {
    if (deferred.contains(partition) or deleting.contains(partition)) {
      // A concurrent retirement already parked it or is deleting its files;
      // that retirement covers the erasure, so this one is redundant.
      retiring.erase(partition);
      erase_marker_if_unreferenced(erasure.marker);
      rp.deliver(atom::done_v);
      return;
    }
    if (in_transformation.contains(partition)) {
      // A transform claimed it in the meantime; erasing its input now would
      // let the data resurrect through the transform's output.
      erase_marker_if_unreferenced(erasure.marker);
      retiring.erase(partition);
      rp.deliver(
        caf::make_error(ec::busy, fmt::format("refusing to erase partition {} "
                                              "while it is being transformed",
                                              partition)));
      return;
    }
    erase(partition);
    if (policy) {
      ++markers_in_disposal[erasure.marker];
      release_marker_after_flush(erasure.marker);
    }
    retiring.erase(partition);
    // Pins may go away while a tombstone write is in flight; a release then
    // found nothing parked and did nothing, so parking now would wait for a
    // trigger that already came and went.
    if (not pin_counts.contains(partition)) {
      dispose_of(partition, std::move(erasure), rp);
      return;
    }
    deferred.emplace(partition, std::move(erasure));
    rp.deliver(atom::done_v);
    advance_maintenance(time::clock::now());
  };
  // Every retirement records its intent first, pinned or not: a crash while
  // the file operations are outstanding -- or an operation that fails --
  // would otherwise resurrect the retired data, since a partition file whose
  // synopsis went missing has its synopsis regenerated at the next startup.
  // For an erasure the marker is a plain tombstone. A quarantine sets the
  // marker's quarantine flag instead: its replay moves the store aside for
  // inspection rather than erasing it, so a crash between the marker write
  // and the store move preserves the evidence -- and does not resurrect a
  // partition whose corrupt store keeps failing reads, which no automatic
  // pass may ever select for a rebuild again.
  // The partition stays in the catalog until the marker is durable; dropping
  // it first and failing the write would leave files on disk that the next
  // startup scans back in, while this process reports the retirement as
  // failed.
  erasure.marker = paths.marker(uuid::random());
  self
    ->mail(atom::write_v, erasure.marker,
           create_marker({partition}, {}, keep_original_partition::no,
                         erasure.quarantine_error.has_value()))
    .request(filesystem, caf::infinite)
    .then(
      [proceed, erasure](atom::ok) mutable {
        proceed(std::move(erasure));
      },
      [this, partition, rp](caf::error& err) mutable {
        retiring.erase(partition);
        eviction_retry_at = time::clock::now() + defaults::disposal_retry_delay;
        // The partition never left the catalog: a failed erase is a no-op, not
        // a limbo state.
        rp.deliver(std::move(err));
        advance_maintenance(time::clock::now());
      });
  return rp;
}

void catalog_state::dispose_of(
  const uuid& partition, deferred_erase entry,
  Option<caf::typed_response_promise<atom::done>> rp) {
  ++storage_generation;
  // Count the files against `deleting` until the filesystem actor is done
  // with them: a database scan that races the deletion still sees them, and
  // without the correction the budget loop would select further victims for
  // bytes that are already on their way out. A failed deletion leaves the
  // entry too -- the bytes then really are still in use, and the loop is
  // right to look elsewhere.
  if (entry.synopsis) {
    const auto footprint = entry.synopsis->store_file.size
                           + entry.synopsis->indexes_file.size
                           + entry.synopsis->sketches_file.size;
    if (footprint > 0) {
      deleting[partition] = footprint;
    }
  }
  // A partition whose synopsis failed to serialize can end up with an empty
  // `resource::url` (see e.g. `active_partition.cpp`'s handling of a failed
  // external `.mdx` write). `path_from_file_url` would then return an empty
  // path, which the filesystem actor resolves as its own root directory,
  // turning an erase or move meant for one file into one that touches the
  // whole database directory. Fall back to the canonical layout instead.
  const auto resolve
    = [&](const resource& res,
          const std::filesystem::path& fallback) -> std::filesystem::path {
    if (not entry.synopsis) {
      return fallback;
    }
    auto result = path_from_file_url(res);
    return result.empty() ? fallback : result;
  };
  const auto partition_path
    = resolve(entry.synopsis ? entry.synopsis->indexes_file : resource{},
              paths.partition(partition));
  const auto synopsis_path
    = resolve(entry.synopsis ? entry.synopsis->sketches_file : resource{},
              paths.synopsis(partition));
  auto store_path = entry.synopsis
                      ? path_from_file_url(entry.synopsis->store_file)
                      : std::filesystem::path{};
  if (store_path.empty()) {
    // A retirement without a synopsis -- an uncataloged partition left behind
    // by an interrupted write -- still owns a store, and finishing without
    // deleting it would orphan `archive/<uuid>.*` forever, counted by every
    // disk-budget scan and reclaimable by nothing. Probe the archive the way
    // the index used to; no known store implementation deviates from the
    // default path scheme.
    auto probe_failed = false;
    for (const auto* extension : {"store", "feather", "parquet"}) {
      auto candidate
        = paths.archive_dir / fmt::format("{}.{}", partition, extension);
      auto probe_error = std::error_code{};
      const auto present = std::filesystem::exists(candidate, probe_error);
      if (probe_error) {
        probe_failed = true;
        continue;
      }
      if (present) {
        store_path = std::move(candidate);
        break;
      }
    }
    if (store_path.empty() and probe_failed) {
      // Absence must be confirmed: reporting success on an unprobed store
      // would erase the tombstone and orphan the store forever. Re-park and
      // retry instead.
      deleting.erase(partition);
      TENZIR_WARN("{} retries disposing of partition {} because it could not "
                  "confirm whether a store remains",
                  *self, partition);
      entry.retry_at = time::clock::now() + defaults::disposal_retry_delay;
      deferred.emplace(partition, std::move(entry));
      if (rp) {
        rp->deliver(caf::make_error(ec::filesystem_error,
                                    fmt::format("could not confirm the store "
                                                "of partition {}",
                                                partition)));
      }
      advance_maintenance(time::clock::now());
      return;
    }
  }
  // Every file the disposal touches reports into one counter, so retirement
  // completes only when the disk is actually clean. A partial failure -- the
  // store gone but the dense index left behind, say -- re-parks the partition:
  // the tombstone stays referenced, the deadline sweep retries the deletion
  // (erasing a path that is already gone succeeds, so retries converge), and a
  // crash in between still replays the erasure from the marker. Failed file
  // operations are retried even when forced disposal of pinned data is off.
  //
  // The marker stays referenced for the whole disposal: nothing in `deferred`
  // points at it while the deletions are in flight, and without the reference
  // a transform delivering in that window would erase the tombstone its
  // inputs still need.
  if (not entry.marker.empty()) {
    ++markers_in_disposal[entry.marker];
  }
  auto release_marker = [this](const std::filesystem::path& marker) {
    if (marker.empty()) {
      return;
    }
    const auto it = markers_in_disposal.find(marker);
    TENZIR_ASSERT(it != markers_in_disposal.end());
    if (--it->second == 0) {
      markers_in_disposal.erase(it);
    }
  };
  const auto operations = size_t{2} + (store_path.empty() ? 0 : 1);
  auto counter = detail::make_fanout_counter(
    operations,
    [this, partition, entry, rp, release_marker]() mutable {
      ++storage_generation;
      deleting.erase(partition);
      release_marker(entry.marker);
      erase_marker_if_unreferenced(entry.marker);
      if (rp) {
        rp->deliver(atom::done_v);
      }
      advance_maintenance(time::clock::now());
    },
    [this, partition, entry, rp, release_marker](caf::error&& err) mutable {
      deleting.erase(partition);
      release_marker(entry.marker);
      TENZIR_WARN("{} failed to dispose of partition {} and will retry: {}",
                  *self, partition, err);
      entry.deadline = deferred_erase_deadline();
      entry.retry_at = time::clock::now() + defaults::disposal_retry_delay;
      deferred.emplace(partition, std::move(entry));
      if (rp) {
        rp->deliver(std::move(err));
      }
      advance_maintenance(time::clock::now());
    });
  auto erase_file
    = [this, partition, counter](const std::filesystem::path& path,
                                 std::string_view what) {
        self->mail(atom::erase_v, path)
          .urgent()
          .request(filesystem, caf::infinite)
          .then(
            [counter, self = self, partition, what](atom::done) {
              TENZIR_TRACE("{} erased {} of partition {} from filesystem",
                           *self, what, partition);
              counter->receive_success();
            },
            [counter, self = self, partition, path, what](caf::error& err) {
              TENZIR_WARN("{} failed to erase {} of partition {} at {}: {}",
                          *self, what, partition, path, err);
              counter->receive_error(std::move(err));
            });
      };
  if (entry.quarantine_error) {
    // Quarantining moves the store aside instead of deleting it -- and it
    // moves *first*: erasing the dense index and synopsis before the move
    // succeeded would, on a crash or a failed move, leave a store that
    // nothing can rediscover until the marker replays at the next startup.
    TENZIR_WARN("{} quarantines partition {} after an error: {}", *self,
                partition, *entry.quarantine_error);
    if (store_path.empty()) {
      // Two operations were counted; there is no store to move.
      TENZIR_WARN("{} cannot quarantine store for partition {}: no store path "
                  "on record",
                  *self, partition);
      erase_file(synopsis_path, "synopsis");
      erase_file(partition_path, "dense indexes");
      return;
    }
    const auto quarantined_path
      = store_path.parent_path() / "quarantined" / store_path.filename();
    auto err = std::error_code{};
    if (std::filesystem::exists(quarantined_path, err)) {
      // A previous attempt already moved the store and failed later --
      // repeating the move from the now-missing source would fail forever,
      // wedging the retry. The move is done; only the index files remain.
      erase_file(synopsis_path, "synopsis");
      erase_file(partition_path, "dense indexes");
      counter->receive_success();
      return;
    }
    err.clear();
    std::filesystem::create_directories(quarantined_path.parent_path(), err);
    if (err) {
      counter->receive_error(caf::make_error(
        ec::filesystem_error,
        fmt::format("failed to create quarantine directory {}: {}",
                    quarantined_path.parent_path(), err.message())));
      counter->receive_success();
      counter->receive_success();
      return;
    }
    self->mail(atom::move_v, store_path, quarantined_path)
      .request(filesystem, caf::infinite)
      .then(
        [counter, erase_file, synopsis_path, partition_path](atom::done) {
          erase_file(synopsis_path, "synopsis");
          erase_file(partition_path, "dense indexes");
          counter->receive_success();
        },
        [counter, this, partition](caf::error& err) {
          TENZIR_WARN("{} failed to quarantine store for partition {}: {}",
                      *self, partition, err);
          counter->receive_error(std::move(err));
          counter->receive_success();
          counter->receive_success();
        });
    return;
  }
  erase_file(synopsis_path, "synopsis");
  erase_file(partition_path, "dense indexes");
  if (store_path.empty()) {
    // No store path on record. That should not happen for a partition the
    // catalog held, so say so rather than pass an empty path to the
    // filesystem actor, which resolves it as its own root and would take the
    // whole database directory with it.
    TENZIR_WARN("{} cannot erase the store of partition {}: no store path on "
                "record",
                *self, partition);
    return;
  }
  self->mail(atom::erase_v, store_path)
    .urgent()
    .request(filesystem, caf::infinite)
    .then(
      [counter](atom::done) {
        counter->receive_success();
      },
      [counter](caf::error& err) {
        counter->receive_error(std::move(err));
      });
}

auto catalog_state::erase_from_disk(const uuid& partition)
  -> caf::result<atom::done> {
  TENZIR_VERBOSE("{} erases partition {}", *self, partition);
  const auto known = find_synopsis(partition) != nullptr;
  if (not known) {
    // A caller may name a partition that never made it into the catalog --
    // one left behind on disk by an interrupted write, say. Deleting it is
    // still the right thing to do, but a partition that is neither known nor
    // on disk is a caller error.
    auto err = std::error_code{};
    const auto path = paths.partition(partition);
    if (not std::filesystem::exists(path, err)) {
      return caf::make_error(ec::logic_error,
                             fmt::format("unknown partition for path {}: {}",
                                         path, err.message()));
    }
  } else if (in_transformation.contains(partition)) {
    // Erasing a partition out from under a transform would let its data
    // resurrect through the transform's output. The budget loop skips such a
    // partition when selecting and picks it up once it is free.
    return caf::make_error(ec::busy,
                           fmt::format("refusing to erase partition {} while "
                                       "it is being transformed",
                                       partition));
  }
  return retire(partition, None{});
}

auto catalog_state::erase_and_extract(const uuid& partition, std::string error)
  -> caf::result<atom::done> {
  if (not find_synopsis(partition)) {
    erase(partition);
    return atom::done_v;
  }
  if (in_transformation.contains(partition)) {
    return caf::make_error(ec::busy,
                           fmt::format("refusing to quarantine partition {} "
                                       "while it is being transformed",
                                       partition));
  }
  return retire(partition, std::move(error));
}
auto catalog_state::lookup(expression expr)
  -> caf::expected<catalog_lookup_result> {
  // The catalog's own engine carries no sketch budget: its internal callers
  // (the rebuild filter, the initial dbstate collection) accept conservative
  // candidate sets, and the configured budget belongs to the query-serving
  // workers.
  return lookup_engine.lookup(std::move(expr), *synopses_per_type);
}

auto catalog_state::memusage() const -> size_t {
  size_t result = 0;
  for (const auto& [type, id_synopsis_map] : *synopses_per_type) {
    for (const auto& [id, synopsis] : *id_synopsis_map) {
      result += synopsis->memusage();
    }
  }
  return result;
}

auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             filesystem_actor filesystem, partition_paths paths,
             std::string store_backend, index_config synopsis_opts,
             size_t partition_capacity, size_t desired_batch_size,
             maintenance_options maintenance, duration deferred_erase_timeout,
             size_t sketch_cache_bytes, bool lazy_sketches,
             size_t lookup_parallelism, node_actor node)
  -> catalog_actor::behavior_type {
  if (self->getf(caf::local_actor::is_detached_flag)) {
    caf::detail::set_thread_name("tnz.catalog");
  }
  self->state().self = self;
  self->state().filesystem = std::move(filesystem);
  self->state().paths = std::move(paths);
  self->state().synopsis_opts = std::move(synopsis_opts);
  // For historic reasons, the `tenzir.max-partition-size` is stored as the
  // `cardinality` in the value index options.
  self->state().index_opts["cardinality"] = partition_capacity;
  // The transformer needs both of these to size its share of the memory
  // budget. Passing them through `index_opts` keeps its spawn signature
  // unchanged.
  if (auto budget = caf::get_if<caf::config_value::integer>(
        &content(self->system().config()), "tenzir.rebuild-memory-budget")) {
    if (*budget < 0) {
      auto error
        = caf::make_error(ec::invalid_configuration,
                          "tenzir.rebuild-memory-budget must not be negative");
      TENZIR_ERROR("{}", render(error));
      self->quit(error);
      return catalog_actor::behavior_type::make_empty_behavior();
    }
    self->state().index_opts["rebuild-memory-budget"] = *budget;
  }
  self->state().index_opts["rebuild-parallelism"]
    = caf::get_or(content(self->system().config()), "tenzir.automatic-rebuild",
                  caf::config_value::integer{1});
  self->state().partition_capacity = partition_capacity;
  self->state().desired_batch_size = desired_batch_size;
  self->state().deferred_erase_timeout = deferred_erase_timeout;
  self->state().store_actor_plugin
    = plugins::find<store_actor_plugin>(store_backend);
  if (not self->state().store_actor_plugin) {
    auto error = caf::make_error(ec::invalid_configuration,
                                 fmt::format("could not find store plugin '{}'",
                                             store_backend));
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return catalog_actor::behavior_type::make_empty_behavior();
  }
  self->state().lookup_engine.taxonomies.concepts = modules::concepts();
  self->state().lazy_sketches = lazy_sketches;
  // Candidate lookups run on a pool of workers so that this actor's thread --
  // which also performs the storage policy's blocking state writes -- never
  // sits between a query and its candidate set. Each worker gets a share of
  // the sketch-cache budget; the caches need no coordination, because
  // partition ids are never reused (the defensive re-merge case is covered by
  // `invalidate_sketches`).
  const auto workers = std::max<size_t>(1, lookup_parallelism);
  self->state().lookup_pool.reserve(workers);
  for (auto i = size_t{0}; i < workers; ++i) {
    // Also stop workers if initialization below exits before a behavior exists.
    self->state().lookup_pool.push_back(self->spawn<caf::linked>(
      catalog_lookup_worker, self->state().lookup_engine.taxonomies,
      sketch_cache_bytes / workers));
  }
  // Load the on-disk state before installing the behavior below. The catalog
  // is detached, so blocking here only delays this actor; everything sent to
  // it in the meantime waits in the mailbox.
  if (auto err = self->state().load_from_disk(); err.valid()) {
    TENZIR_ERROR("{} failed to load its state from disk: {}", *self,
                 render(err));
    self->quit(std::move(err));
    return catalog_actor::behavior_type::make_empty_behavior();
  }
  TENZIR_VERBOSE("{} finished initializing and is ready to accept queries",
                 *self);
  // The rebuild metrics. The importer is not up yet when the catalog starts,
  // so the periodic emitter below looks it up each time, the way the index
  // does for its own actor metrics.
  {
    self->state().quarantine_metric = series_builder{type{
      "tenzir.metrics.rebuild_quarantine",
      record_type{
        {"timestamp", time_type{}},
        {"partition", string_type{}},
        {"error", string_type{}},
      },
      {{"internal"}},
    }};
    auto builder = series_builder{type{
      "tenzir.metrics.rebuild",
      record_type{
        {"timestamp", time_type{}},
        {"partitions", uint64_type{}},
        {"queued_partitions", uint64_type{}},
      },
      {{"internal"}},
    }};
    detail::weak_run_delayed_loop(
      self, defaults::metrics_interval,
      [self, builder = std::move(builder)]() mutable {
        const auto importer
          = self->system().registry().get<importer_actor>("tenzir.importer");
        if (not importer) {
          return;
        }
        const auto& rebuild = self->state().rebuild;
        auto metric = builder.record();
        metric.field("timestamp", time::clock::now());
        // Partitions, not batches: `running` counts batches in flight, and
        // the field has always meant partitions.
        metric.field("partitions", rebuild ? rebuild->running_partitions : 0);
        // The catalog selects a batch at a time against live state, so there
        // is no queue to report. Always zero, by construction.
        metric.field("queued_partitions", uint64_t{0});
        self->mail(builder.finish_assert_one_slice()).send(importer);
      });
  }
  self->state().maintenance = maintenance;
  if (auto error = self->state().make_policy(); error.valid()) {
    TENZIR_ERROR("{} failed to replay storage policy state: {}", *self,
                 render(error));
    self->quit(std::move(error));
    return catalog_actor::behavior_type::make_empty_behavior();
  }
  if (auto error = self->state().initialize_maintenance(time::clock::now());
      error.valid()) {
    self->quit(std::move(error));
    return catalog_actor::behavior_type::make_empty_behavior();
  }
  auto start_maintenance = [self] {
    self->state().maintenance_ready = true;
    self->state().advance_maintenance(time::clock::now());
  };
  if (node and plugins::find<component_plugin>("package-manager")) {
    // The node answers only after creating its components. A status reply
    // then establishes that the package manager finished initialization and
    // published its operators. Keep serving catalog requests while waiting:
    // component startup itself may need them.
    auto fail_startup = [self](caf::error const& error) {
      self->quit(diagnostic::error(error)
                   .note("waiting for package operators before starting "
                         "catalog maintenance")
                   .to_error());
    };
    self
      ->mail(atom::get_v, atom::label_v,
             std::vector<std::string>{"package-manager"})
      .request(node, caf::infinite)
      .then(
        [self, start_maintenance,
         fail_startup](std::vector<caf::actor> const& components) {
          TENZIR_ASSERT(components.size() == 1);
          auto packages
            = caf::actor_cast<component_plugin_actor>(components[0]);
          self->mail(atom::status_v, status_verbosity::info, duration::zero())
            .request(packages, caf::infinite)
            .then(
              [start_maintenance](record const&) {
                start_maintenance();
              },
              fail_startup);
        },
        fail_startup);
  } else {
    start_maintenance();
  }
  return {
    [self](atom::merge, std::vector<partition_synopsis_pair>& partitions)
      -> caf::result<atom::ok> {
      // Only the index sends this message, once per persisted ingest
      // partition. Transform outputs are merged inside the apply handler
      // instead, which is what keeps the partition creation listeners tied to
      // ingest.
      auto notification = self->state().partition_creation_listeners.empty()
                            ? std::vector<partition_synopsis_pair>{}
                            : partitions;
      if (const auto& policy = self->state().policy) {
        // Ingest only. A transform's outputs reach the policy through
        // `on_committed`, so that a policy keeping per-partition state is not
        // fed the same partition twice.
        for (const auto& partition : partitions) {
          policy->on_merged(partition);
        }
      }
      auto const now = time::clock::now();
      auto result = self->state().merge(std::move(partitions),
                                        catalog_state::merge_source::ingest);
      self->state().advance_maintenance(now);
      for (const auto& listener : self->state().partition_creation_listeners) {
        self->mail(atom::update_v, notification).send(listener);
      }
      return result;
    },
    [self](atom::apply, ast::pipeline& pipe,
           std::vector<partition_info>& selected, keep_original_partition keep,
           std::string& origin,
           std::string& policy_token) -> caf::result<partition_apply_result> {
      return self->state().apply(std::move(pipe), std::move(selected), keep,
                                 std::move(origin), std::move(policy_token));
    },
    [self](atom::subscribe, atom::create,
           const partition_creation_listener_actor& listener,
           send_initial_dbstate should_send) -> caf::result<void> {
      TENZIR_DEBUG("{} adds partition creation listener", *self);
      self->state().add_partition_creation_listener(listener);
      if (should_send == send_initial_dbstate::yes) {
        self->mail(atom::update_v, collect_synopses(self->state()))
          .send(listener);
      }
      return {};
    },
    [self](atom::get) -> caf::result<std::vector<partition_synopsis_pair>> {
      return collect_synopses(self->state());
    },
    [self](atom::get, const expression& filter)
      -> caf::result<std::vector<partition_synopsis_pair>> {
      return collect_synopses(self->state(), filter);
    },
    [self](atom::get, const std::string& selector)
      -> caf::result<std::vector<table_slice>> {
      const auto parsed = from_string<catalog_slice_selector>(selector);
      if (not parsed) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("unsupported catalog get selector: "
                                           "{}",
                                           selector));
      }
      return build_catalog_slices(*parsed, collect_synopses(self->state()));
    },
    [self](atom::get, const std::string& selector,
           const expression& filter) -> caf::result<std::vector<table_slice>> {
      const auto parsed = from_string<catalog_slice_selector>(selector);
      if (not parsed) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("unsupported catalog get selector: "
                                           "{}",
                                           selector));
      }
      if (*parsed != catalog_slice_selector::partitions) {
        return caf::make_error(ec::invalid_argument,
                               fmt::format("filtered catalog get is only "
                                           "supported for partitions, got {}",
                                           selector));
      }
      const auto synopses = collect_synopses(self->state(), filter);
      if (not synopses) {
        return synopses.error();
      }
      return build_catalog_slices(*parsed, *synopses);
    },
    [self](atom::erase, uuid partition) -> caf::result<atom::done> {
      return self->state().erase_from_disk(partition);
    },
    [self](atom::erase,
           const std::vector<uuid>& partitions) -> caf::result<atom::done> {
      if (partitions.empty()) {
        return atom::done_v;
      }
      auto rp = self->make_response_promise<atom::done>();
      auto counter = detail::make_fanout_counter(
        partitions.size(),
        [rp]() mutable {
          rp.deliver(atom::done_v);
        },
        [rp](caf::error&& err) mutable {
          rp.deliver(std::move(err));
        });
      for (const auto& partition : partitions) {
        self->mail(atom::erase_v, partition)
          .request(static_cast<catalog_actor>(self), caf::infinite)
          .then(
            [counter](atom::done) {
              counter->receive_success();
            },
            [counter](caf::error& err) {
              counter->receive_error(std::move(err));
            });
      }
      return rp;
    },
    [self](atom::erase, atom::extract, uuid partition,
           std::string& error) -> caf::result<atom::done> {
      return self->state().erase_and_extract(partition, std::move(error));
    },
    [self](atom::replace, const std::vector<uuid>& old_uuids,
           std::vector<partition_synopsis_pair>& new_synopses)
      -> caf::result<atom::ok> {
      if (auto const& policy = self->state().policy) {
        auto outputs = std::vector<partition_info>{};
        for (auto const& [id, synopsis] : new_synopses) {
          outputs.emplace_back(id, *synopsis);
        }
        policy->on_replaced(old_uuids, outputs);
      }
      for (auto const& uuid : old_uuids) {
        self->state().erase(uuid, catalog_state::notify_policy::no);
      }
      auto result = self->state().merge(std::move(new_synopses));
      self->state().advance_maintenance(time::clock::now());
      return result;
    },
    [self](atom::candidates, tenzir::query_context query_context)
      -> caf::result<catalog_lookup_result> {
      auto& state = self->state();
      // Evaluation runs on a worker against a snapshot, so this thread never
      // sits behind predicate evaluation or on-demand sketch loading. The
      // whole snapshot is leased provisionally before delegating: a partition
      // retired while the worker evaluates is thereby parked instead of
      // deleted, so a candidate the reply hands out is still openable -- the
      // same guarantee the synchronous lookup gave by pinning at reply time.
      // The lease narrows to the actual candidates when the result arrives.
      auto snapshot = catalog_snapshot{state.synopses_per_type};
      auto all_ids = std::vector<uuid>{};
      for (const auto& [schema, entries] : *snapshot.synopses) {
        for (const auto& [id, synopsis] : *entries) {
          all_ids.push_back(id);
        }
      }
      const auto query = query_context.id;
      const auto generation
        = state.add_lease(query, self->current_sender(), std::move(all_ids));
      auto rp = self->make_response_promise<catalog_lookup_result>();
      self
        ->mail(atom::candidates_v, std::move(query_context.expr),
               std::move(snapshot))
        .request(state.next_lookup_worker(), caf::infinite)
        .then(
          [self, rp, query, generation](catalog_lookup_result& result) mutable {
            self->state().narrow_lease(query, generation,
                                       partition_ids(result));
            rp.deliver(std::move(result));
          },
          [self, rp, query, generation](caf::error& error) mutable {
            self->state().narrow_lease(query, generation, {});
            rp.deliver(std::move(error));
          });
      return rp;
    },
    [self](atom::start, atom::rebuild,
           rebuild_options& options) -> caf::result<void> {
      return self->state().start_rebuild(std::move(options));
    },
    [self](atom::stop, atom::rebuild,
           const rebuild_stop_options& options) -> caf::result<void> {
      return self->state().stop_rebuild(options);
    },
    [self](atom::release, uuid query) -> caf::result<void> {
      self->state().release_lease(query);
      return {};
    },
    [self](atom::release, uuid query,
           const std::vector<uuid>& partitions) -> caf::result<void> {
      self->state().release_lease(query, partitions);
      return {};
    },
    [self](atom::get, uuid uuid) -> caf::result<partition_info> {
      for (const auto& [type, synopses] : *self->state().synopses_per_type) {
        if (auto it = synopses->find(uuid); it != synopses->end()) {
          return partition_info{uuid, *it->second};
        }
      }
      return caf::make_error(
        tenzir::ec::lookup_error,
        fmt::format("unable to find partition with uuid: {}", uuid));
    },
    [self](atom::run, atom::compaction, std::string& rule,
           Option<duration> older_than,
           Option<duration> newer_than) -> caf::result<atom::done> {
      return self->state().run_named_rule(std::move(rule), older_than,
                                          newer_than);
    },
    [self](atom::list, atom::compaction) -> caf::result<record> {
      if (not self->state().policy) {
        return caf::make_error(ec::invalid_configuration,
                               "no storage policy is configured");
      }
      return self->state().policy->describe();
    },
    [self](atom::status, status_verbosity, duration) {
      auto result = self->state().active_transformations_status();
      if (auto rebuild = self->state().rebuild_status(); not rebuild.empty()) {
        result["rebuild"] = std::move(rebuild);
      }
      if (auto space = self->state().space_status(); not space.empty()) {
        result["space"] = std::move(space);
      }
      return result;
    },
    [self](const caf::exit_msg& msg) {
      TENZIR_VERBOSE("{} received EXIT from {} with reason: {}", *self,
                     msg.source, msg.reason);
      // Pending policy state becomes durable before teardown; the write is
      // synchronous, so nothing can outrun it.
      if (auto error = self->state().flush_policy_markers(); error.valid()) {
        TENZIR_WARN("{} failed to flush its storage policy during "
                    "shutdown: {}",
                    *self, error);
      }
      auto dependents = std::vector<caf::actor>{};
      dependents.reserve(self->state().active_transformers.size()
                         + self->state().lookup_pool.size());
      for (const auto& worker : self->state().lookup_pool) {
        self->unlink_from(worker);
        dependents.push_back(caf::actor_cast<caf::actor>(worker));
      }
      for (auto& [addr, disposable] : self->state().active_transformers) {
        disposable.dispose();
        dependents.push_back(caf::actor_cast<caf::actor>(addr));
      }
      shutdown<policy::parallel>(self, std::move(dependents), msg.reason);
    },
  };
}

} // namespace tenzir
