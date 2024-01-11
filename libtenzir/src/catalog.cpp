//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/as_bytes.hpp"
#include "tenzir/catalog_lookup.hpp"
#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/set_operations.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/report.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/taxonomies.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>

#include <type_traits>

namespace tenzir {

void catalog_state::create_from(
  std::unordered_map<uuid, partition_synopsis_ptr>&& ps) {
  if (not partitions.empty()) {
    throw std::logic_error(
      fmt::format("{} must be empty when loading partitions", *self));
  }
  for (auto&& [uuid, synopsis] : std::move(ps)) {
    update_unprunable_fields(*synopsis);
    partitions.emplace_back(uuid, std::move(synopsis));
  }
  std::sort(
    partitions.begin(), partitions.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.synopsis->max_import_time < rhs.synopsis->max_import_time;
    });
}

void catalog_state::merge(const uuid& uuid, partition_synopsis_ptr synopsis) {
  update_unprunable_fields(*synopsis);
  const auto first_older
    = std::upper_bound(partitions.begin(), partitions.end(),
                       synopsis->max_import_time,
                       [](const auto ts, const auto& partition) {
                         return partition.synopsis->max_import_time > ts;
                       });
  partitions.emplace(first_older, uuid, std::move(synopsis));
  TENZIR_ASSERT_EXPENSIVE(std::is_sorted(
    partitions.begin(), partitions.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.synopsis->max_import_time < rhs.synopsis->max_import_time;
    }));
}

void catalog_state::erase(const uuid& uuid) {
  const auto it = std::remove_if(partitions.begin(), partitions.end(),
                                 [&](const auto& partition) {
                                   return partition.uuid == uuid;
                                 });
  partitions.erase(it, partitions.end());
}

void catalog_state::replace(
  const std::vector<uuid>& old_uuids,
  std::vector<partition_synopsis_pair> new_partitions) {
  const auto it = std::remove_if(
    partitions.begin(), partitions.end(), [&](const auto& partition) {
      return std::any_of(old_uuids.begin(), old_uuids.end(),
                         [&](const auto& uuid) {
                           return uuid == partition.uuid;
                         });
    });
  partitions.erase(it, partitions.end());
  for (auto&& partition : std::move(new_partitions)) {
    merge(partition.uuid, std::move(partition.synopsis));
  }
}

size_t catalog_state::memusage() const {
  size_t result = 0;
  for (const auto& partition : partitions) {
    result += partition.synopsis->memusage();
  }
  return result;
}

void catalog_state::update_unprunable_fields(const partition_synopsis& ps) {
  for (auto const& [field, synopsis] : ps.field_synopses_)
    if (synopsis != nullptr
        && caf::holds_alternative<string_type>(field.type()))
      unprunable_fields.insert(std::string{field.name()});
}

type_set catalog_state::schemas() const {
  auto result = type_set{};
  for (const auto& partition : partitions)
    result.insert(partition.synopsis->schema);
  return result;
}

void catalog_state::emit_metrics() const {
  TENZIR_ASSERT(accountant);
  auto num_partitions_and_events_per_schema_and_version
    = detail::stable_map<std::pair<std::string_view, uint64_t>,
                         std::pair<uint64_t, uint64_t>>{};
  for (const auto& partition : partitions) {
    auto& [num_partitions, num_events]
      = num_partitions_and_events_per_schema_and_version[std::pair{
        partition.synopsis->schema.name(), partition.synopsis->version}];
    num_partitions += 1;
    num_events += partition.synopsis->events;
  }
  auto total_num_partitions = uint64_t{0};
  auto total_num_events = uint64_t{0};
  auto r = report{};
  r.data.reserve(num_partitions_and_events_per_schema_and_version.size());
  for (const auto& [schema_and_version, num_partitions_and_events] :
       num_partitions_and_events_per_schema_and_version) {
    auto [schema_num_partitions, schema_num_events] = num_partitions_and_events;
    total_num_partitions += schema_num_partitions;
    total_num_events += schema_num_events;
    r.data.push_back(data_point{
          .key = "catalog.num-partitions",
          .value = schema_num_partitions,
          .metadata = {
            {"schema", std::string{schema_and_version.first}},
            {"partition-version", fmt::to_string(schema_and_version.second)},
          },
        });
    r.data.push_back(data_point{
          .key = "catalog.num-events",
          .value = schema_num_events,
          .metadata = {
            {"schema", std::string{schema_and_version.first}},
            {"partition-version", fmt::to_string(schema_and_version.second)},
          },
        });
  }
  r.data.push_back(data_point{
    .key = "catalog.num-partitions-total",
    .value = total_num_partitions,
  });
  r.data.push_back(data_point{
    .key = "catalog.num-events-total",
    .value = total_num_events,
  });
  r.data.push_back(data_point{
          .key = "memory-usage",
          .value = memusage(),
          .metadata = {
            {"component", std::string{name}},
          },
        });
  self->send(accountant, atom::metrics_v, std::move(r));
}

catalog_actor::behavior_type
catalog(catalog_actor::stateful_pointer<catalog_state> self,
        accountant_actor accountant) {
  if (self->getf(caf::local_actor::is_detached_flag))
    caf::detail::set_thread_name("tenzir.catalog");
  self->state.self = self;
  self->state.taxonomies.concepts = modules::concepts();
  //  Load loaded schema types from the singleton.
  if (accountant) {
    self->state.accountant = std::move(accountant);
    self->send(self->state.accountant, atom::announce_v, self->name());
    detail::weak_run_delayed_loop(self, defaults::telemetry_rate, [self] {
      self->state.emit_metrics();
    });
  }
  return {
    [self](atom::merge,
           std::shared_ptr<std::unordered_map<uuid, partition_synopsis_ptr>>& ps)
      -> caf::result<atom::ok> {
      auto unsupported_partitions = std::vector<uuid>{};
      for (const auto& [uuid, synopsis] : *ps) {
        auto supported
          = version::support_for_partition_version(synopsis->version);
        if (supported.end_of_life)
          unsupported_partitions.push_back(uuid);
      }
      if (!unsupported_partitions.empty()) {
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
      self->state.create_from(std::move(*ps));
      return atom::ok_v;
    },
    [self](atom::merge, uuid partition,
           partition_synopsis_ptr& synopsis) -> atom::ok {
      TENZIR_TRACE_SCOPE("{} {}", *self, TENZIR_ARG(partition));
      self->state.merge(partition, std::move(synopsis));
      return atom::ok_v;
    },
    [self](
      atom::merge,
      std::vector<partition_synopsis_pair>& partition_synopses) -> atom::ok {
      for (auto& [uuid, partition_synopsis] : partition_synopses)
        self->state.merge(uuid, partition_synopsis);
      return atom::ok_v;
    },
    [self](atom::get) {
      return std::vector<partition_synopsis_pair>{
        self->state.partitions.begin(), self->state.partitions.end()};
    },
    [self](atom::get, atom::type) {
      TENZIR_TRACE_SCOPE("{} retrieves a list of all known types", *self);
      return self->state.schemas();
    },
    [self](atom::erase, uuid partition) {
      self->state.erase(partition);
      return atom::ok_v;
    },
    [self](atom::replace, const std::vector<uuid>& old_uuids,
           std::vector<partition_synopsis_pair>& new_partitions) {
      self->state.replace(old_uuids, std::move(new_partitions));
      return atom::ok_v;
    },
    [self](atom::candidates,
           query_context& query) -> caf::result<catalog_lookup_actor> {
      const auto cache_capacity = uint64_t{3};
      return self->spawn(make_catalog_lookup, self->state.partitions,
                         self->state.unprunable_fields, self->state.taxonomies,
                         std::move(query), cache_capacity);
    },
    [self](atom::internal, atom::candidates,
           query_context& query) -> caf::result<legacy_catalog_lookup_result> {
      const auto start = std::chrono::steady_clock::now();
      const auto cache_capacity = uint64_t{self->state.partitions.size()};
      TENZIR_WARN("spawn lookup helper");
      auto catalog_lookup
        = self->spawn(make_catalog_lookup, self->state.partitions,
                      self->state.unprunable_fields, self->state.taxonomies,
                      std::move(query), cache_capacity);
      auto blocking_self = caf::scoped_actor{self->system()};
      auto num_candidates = size_t{0};
      auto result = legacy_catalog_lookup_result{};
      while (true) {
        TENZIR_WARN("use lookup helper");
        auto partial_results = std::vector<catalog_lookup_result>{};
        auto error = caf::error{};
        blocking_self->request(catalog_lookup, caf::infinite, atom::get_v)
          .receive(
            [&](std::vector<catalog_lookup_result>& response) {
              num_candidates += response.size();
              TENZIR_WARN("received {}=>{} results", response.size(),
                          num_candidates);
              partial_results = std::move(response);
            },
            [&](caf::error& response) {
              TENZIR_WARN("received error: {}", response);
              error = std::move(response);
            });
        if (error) {
          return error;
        }
        if (partial_results.empty()) {
          break;
        }
        for (auto& partial_result : partial_results) {
          auto& entry = result.candidate_infos[partial_result.partition.schema];
          if (entry.partition_infos.empty()) {
            entry.exp = std::move(partial_result.bound_expr);
          } else {
            TENZIR_ASSERT_EXPENSIVE(entry.exp == partial_result.bound_expr);
          }
          entry.partition_infos.push_back(std::move(partial_result.partition));
        }
      }
      const auto id_str = fmt::to_string(query.id);
      const duration runtime = std::chrono::steady_clock::now() - start;
      self->send(self->state.accountant, atom::metrics_v,
                 "catalog.lookup.runtime", runtime,
                 metrics_metadata{
                   {"query", id_str},
                   {"issuer", query.issuer},
                 });
      self->send(self->state.accountant, atom::metrics_v,
                 "catalog.lookup.candidates", num_candidates,
                 metrics_metadata{
                   {"query", std::move(id_str)},
                   {"issuer", query.issuer},
                 });
      return result;
    },
    [self](atom::get, uuid uuid) -> caf::result<partition_info> {
      for (const auto& partition : self->state.partitions) {
        if (partition.uuid == uuid) {
          return partition_info{uuid, *partition.synopsis};
        }
      }
      return caf::make_error(
        tenzir::ec::lookup_error,
        fmt::format("unable to find partition with uuid: {}", uuid));
    },
    [](atom::status, status_verbosity, duration) {
      return record{};
    },
    [self](atom::get, atom::taxonomies) {
      return self->state.taxonomies;
    },
  };
}

} // namespace tenzir
