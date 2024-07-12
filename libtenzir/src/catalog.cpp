//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/set_operations.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/prune.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/report.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/time_synopsis.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/expected.hpp>

namespace tenzir {

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

auto catalog_state::initialize(
  std::shared_ptr<std::unordered_map<uuid, partition_synopsis_ptr>> ps)
  -> caf::result<atom::ok> {
  auto unsupported_partitions = std::vector<uuid>{};
  for (const auto& [uuid, synopsis] : *ps) {
    auto supported = version::support_for_partition_version(synopsis->version);
    if (supported.end_of_life) {
      unsupported_partitions.push_back(uuid);
    }
  }
  if (!unsupported_partitions.empty()) {
    return caf::make_error(
      ec::version_error,
      fmt::format("{} cannot load unsupported partitions; please run "
                  "'tenzir "
                  "rebuild' with at least {} to rebuild the following "
                  "partitions, or delete them from the database directory: "
                  "{}",
                  *self,
                  version::support_for_partition_version(
                    version::current_partition_version)
                    .introduced,
                  fmt::join(unsupported_partitions, ", ")));
  }
  auto flat_data_map = std::unordered_map<
    tenzir::type, std::vector<std::pair<uuid, partition_synopsis_ptr>>>{};
  for (auto& [uuid, synopsis] : *ps) {
    TENZIR_ASSERT(synopsis->get_reference_count() == 1ull);
    update_unprunable_fields(*synopsis);
    flat_data_map[synopsis->schema].emplace_back(uuid, std::move(synopsis));
  }
  for (auto& [type, flat_data] : flat_data_map) {
    std::sort(flat_data.begin(), flat_data.end(),
              [](const std::pair<uuid, partition_synopsis_ptr>& lhs,
                 const std::pair<uuid, partition_synopsis_ptr>& rhs) {
                return lhs.first < rhs.first;
              });
    synopses_per_type[type]
      = decltype(synopses_per_type)::value_type::second_type::make_unsafe(
        std::move(flat_data));
  }
  return atom::ok_v;
}

auto catalog_state::merge(std::vector<partition_synopsis_pair> partitions)
  -> caf::result<atom::ok> {
  for (auto& [id, synopsis] : partitions) {
    update_unprunable_fields(*synopsis);
    auto& entry = synopses_per_type[synopsis->schema][id];
    entry = std::move(synopsis);
  }
  return atom::ok_v;
}

void catalog_state::erase(const uuid& partition) {
  for (auto& [type, uuid_synopsis_map] : synopses_per_type) {
    const auto num_erased = uuid_synopsis_map.erase(partition);
    if (num_erased > 0) {
      if (uuid_synopsis_map.empty()) {
        synopses_per_type.erase(type);
      }
      return;
    }
  }
}

auto catalog_state::lookup(expression expr) const
  -> caf::expected<catalog_lookup_result> {
  auto start = stopwatch::now();
  auto total_candidates = catalog_lookup_result{};
  auto num_candidate_partitions = size_t{0};
  auto num_candidate_events = size_t{0};
  if (expr == caf::none) {
    expr = trivially_true_expression();
  }
  auto normalized = normalize_and_validate(expr);
  if (not normalized) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("{} failed to normalize and validate "
                                       "epxression {}: {}",
                                       *self, expr, normalized.error()));
  }
  for (const auto& [type, _] : synopses_per_type) {
    auto resolved = resolve(taxonomies, *normalized, type);
    if (not resolved) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} failed to resolve epxression {}: "
                                         "{}",
                                         *self, expr, resolved.error()));
    }
    auto pruned = prune(*resolved, unprunable_fields);
    auto candidates_per_type = lookup_impl(pruned, type);
    // Sort partitions by their max import time, returning the most recent
    // partitions first.
    std::sort(candidates_per_type.partition_infos.begin(),
              candidates_per_type.partition_infos.end(),
              [&](const partition_info& lhs, const partition_info& rhs) {
                return lhs.max_import_time > rhs.max_import_time;
              });
    num_candidate_partitions += candidates_per_type.partition_infos.size();
    num_candidate_events
      += std::transform_reduce(candidates_per_type.partition_infos.begin(),
                               candidates_per_type.partition_infos.end(),
                               size_t{0}, std::plus<>{},
                               [](const auto& partition) {
                                 return partition.events;
                               });
    total_candidates.candidate_infos[type] = std::move(candidates_per_type);
  }
  auto delta = std::chrono::duration_cast<std::chrono::microseconds>(
    stopwatch::now() - start);
  TENZIR_VERBOSE("catalog found {} candidate partitions ({} events) in "
                 "{} microseconds",
                 num_candidate_partitions, num_candidate_events, delta.count());
  TENZIR_TRACEPOINT(catalog_lookup, delta.count(), num_candidate_partitions);
  return total_candidates;
}

auto catalog_state::lookup_impl(const expression& expr,
                                const type& schema) const
  -> catalog_lookup_result::candidate_info {
  TENZIR_ASSERT(!caf::holds_alternative<caf::none_t>(expr));
  auto synopsis_map_per_type_it = synopses_per_type.find(schema);
  TENZIR_ASSERT(synopsis_map_per_type_it != synopses_per_type.end());
  const auto& partition_synopses = synopsis_map_per_type_it->second;
  // The partition UUIDs must be sorted, otherwise the invariants of the
  // inplace union and intersection algorithms are violated, leading to
  // wrong results. So all places where we return an assembled set must
  // ensure the post-condition of returning a sorted list. We currently
  // rely on `flat_map` already traversing them in the correct order, so
  // no separate sorting step is required.
  auto memoized_partitions = catalog_lookup_result::candidate_info{};
  auto all_partitions = [&] {
    if (!memoized_partitions.partition_infos.empty()
        || partition_synopses.empty())
      return memoized_partitions;
    for (const auto& [partition_id, synopsis] : partition_synopses) {
      memoized_partitions.partition_infos.emplace_back(partition_id, *synopsis);
    }
    return memoized_partitions;
  };
  auto f = detail::overload{
    [&](const conjunction& x) -> catalog_lookup_result::candidate_info {
      TENZIR_ASSERT(!x.empty());
      auto i = x.begin();
      auto result = lookup_impl(*i, schema);
      if (!result.partition_infos.empty())
        for (++i; i != x.end(); ++i) {
          // TODO: A conjunction means that we can restrict the lookup to the
          // remaining candidates. This could be achived by passing the `result`
          // set to `lookup` along with the child expression.
          auto xs = lookup_impl(*i, schema);
          if (xs.partition_infos.empty())
            return xs; // short-circuit
          detail::inplace_intersect(result.partition_infos, xs.partition_infos);
          TENZIR_ASSERT_EXPENSIVE(std::is_sorted(result.partition_infos.begin(),
                                                 result.partition_infos.end()));
        }
      return result;
    },
    [&](const disjunction& x) -> catalog_lookup_result::candidate_info {
      catalog_lookup_result::candidate_info result;
      for (const auto& op : x) {
        // TODO: A disjunction means that we can restrict the lookup to the
        // set of partitions that are outside of the current result set.
        auto xs = lookup_impl(op, schema);
        if (xs.partition_infos.size() == partition_synopses.size())
          return xs; // short-circuit
        TENZIR_ASSERT_EXPENSIVE(
          std::is_sorted(xs.partition_infos.begin(), xs.partition_infos.end()));
        detail::inplace_unify(result.partition_infos, xs.partition_infos);
        TENZIR_ASSERT_EXPENSIVE(std::is_sorted(result.partition_infos.begin(),
                                               result.partition_infos.end()));
      }
      return result;
    },
    [&](const negation&) -> catalog_lookup_result::candidate_info {
      // We cannot handle negations, because a synopsis may return false
      // positives, and negating such a result may cause false
      // negatives.
      // TODO: The above statement seems to only apply to bloom filter
      // synopses, but it should be possible to handle time or bool synopses.
      return all_partitions();
    },
    [&](const predicate& x) -> catalog_lookup_result::candidate_info {
      // Performs a lookup on all *matching* synopses with operator and
      // data from the predicate of the expression. The match function
      // uses a qualified_record_field to determine whether the synopsis
      // should be queried.
      auto search = [&](auto match) {
        TENZIR_ASSERT(caf::holds_alternative<data>(x.rhs));
        const auto& rhs = caf::get<data>(x.rhs);
        catalog_lookup_result::candidate_info result;
        // dont iterate through all synopses, rewrite lookup_impl to use a
        // singular type all synopses loops -> relevant anymore? Use type as
        // synopses key
        for (const auto& [part_id, part_syn] : partition_synopses) {
          for (const auto& [field, syn] : part_syn->field_synopses_) {
            if (match(field)) {
              // We need to prune the type's metadata here by converting it to
              // a concrete type and back, because the type synopses are
              // looked up independent from names and attributes.
              auto prune = [&]<concrete_type T>(const T& x) {
                return type{x};
              };
              auto cleaned_type = caf::visit(prune, field.type());
              // We rely on having a field -> nullptr mapping here for the
              // fields that don't have their own synopsis.
              if (syn) {
                auto opt = syn->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  TENZIR_TRACE("{} selects {} at predicate {}",
                               detail::pretty_type_name(this), part_id, x);
                  result.partition_infos.emplace_back(part_id, *part_syn);
                  break;
                }
                // The field has no dedicated synopsis. Check if there is one
                // for the type in general.
              } else if (auto it = part_syn->type_synopses_.find(cleaned_type);
                         it != part_syn->type_synopses_.end() && it->second) {
                auto opt = it->second->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  TENZIR_TRACE("{} selects {} at predicate {}",
                               detail::pretty_type_name(this), part_id, x);
                  result.partition_infos.emplace_back(part_id, *part_syn);
                  break;
                }
              } else {
                // The catalog couldn't rule out this partition, so we have
                // to include it in the result set.
                result.partition_infos.emplace_back(part_id, *part_syn);
                break;
              }
            }
          }
        }
        TENZIR_DEBUG("{} checked {} partitions for predicate {} and got {} "
                     "results",
                     detail::pretty_type_name(this), synopses_per_type.size(),
                     x, result.partition_infos.size());
        // Some calling paths require the result to be sorted.
        TENZIR_ASSERT_EXPENSIVE(std::is_sorted(result.partition_infos.begin(),
                                               result.partition_infos.end()));
        return result;
      };
      auto extract_expr = detail::overload{
        [&](const meta_extractor& lhs,
            const data& d) -> catalog_lookup_result::candidate_info {
          switch (lhs.kind) {
            case meta_extractor::schema: {
              // We don't have to look into the synopses for type queries, just
              // at the schema names.
              catalog_lookup_result::candidate_info result;
              for (const auto& [part_id, part_syn] : partition_synopses) {
                for (const auto& [fqf, _] : part_syn->field_synopses_) {
                  // TODO: provide an overload for view of evaluate() so that
                  // we can use string_view here. Fortunately type names are
                  // short, so we're probably not hitting the allocator due to
                  // SSO.
                  if (evaluate(std::string{fqf.schema_name()}, x.op, d)) {
                    result.partition_infos.emplace_back(part_id, *part_syn);
                    break;
                  }
                }
              }
              TENZIR_ASSERT_EXPENSIVE(std::is_sorted(
                result.partition_infos.begin(), result.partition_infos.end()));
              return result;
            }
            case meta_extractor::schema_id: {
              auto result = catalog_lookup_result::candidate_info{};
              for (const auto& [part_id, part_syn] : partition_synopses) {
                TENZIR_ASSERT_EXPENSIVE(part_syn->schema == schema);
              }
              if (evaluate(schema.make_fingerprint(), x.op, d)) {
                for (const auto& [part_id, part_syn] : partition_synopses) {
                  result.partition_infos.emplace_back(part_id, *part_syn);
                }
              }
              TENZIR_ASSERT_EXPENSIVE(std::is_sorted(
                result.partition_infos.begin(), result.partition_infos.end()));
              return result;
            }
            case meta_extractor::import_time: {
              catalog_lookup_result::candidate_info result;
              for (const auto& [part_id, part_syn] : partition_synopses) {
                TENZIR_ASSERT(
                  part_syn->min_import_time <= part_syn->max_import_time,
                  "encountered empty or moved-from partition synopsis");
                auto ts = time_synopsis{
                  part_syn->min_import_time,
                  part_syn->max_import_time,
                };
                auto add = ts.lookup(x.op, caf::get<tenzir::time>(d));
                if (!add || *add) {
                  result.partition_infos.emplace_back(part_id, *part_syn);
                }
              }
              TENZIR_ASSERT_EXPENSIVE(std::is_sorted(
                result.partition_infos.begin(), result.partition_infos.end()));
              return result;
            }
            case meta_extractor::internal: {
              auto result = catalog_lookup_result::candidate_info{};
              for (const auto& [part_id, part_syn] : partition_synopses) {
                auto internal = false;
                if (part_syn->schema) {
                  internal = part_syn->schema.attribute("internal").has_value();
                }
                if (evaluate(internal, x.op, d)) {
                  result.partition_infos.emplace_back(part_id, *part_syn);
                }
              };
              TENZIR_ASSERT_EXPENSIVE(std::is_sorted(
                result.partition_infos.begin(), result.partition_infos.end()));
              return result;
            }
          }
          TENZIR_WARN("{} cannot process meta extractor: {}",
                      detail::pretty_type_name(this), lhs.kind);
          return all_partitions();
        },
        [&](const field_extractor& lhs,
            const data& d) -> catalog_lookup_result::candidate_info {
          auto pred = [&](const auto& field) {
            auto match_name = [&] {
              auto field_name = field.field_name();
              auto key = std::string_view{lhs.field};
              if (field_name.length() >= key.length()) {
                auto pos = field_name.length() - key.length();
                auto sub = field_name.substr(pos);
                return sub == key && (pos == 0 || field_name[pos - 1] == '.');
              }
              auto schema_name = field.schema_name();
              if (key.length() > schema_name.length() + 1 + field_name.length())
                return false;
              auto pos = key.length() - field_name.length();
              auto second = key.substr(pos);
              if (second != field_name)
                return false;
              if (key[pos - 1] != '.')
                return false;
              auto fpos = schema_name.length() - (pos - 1);
              return key.substr(0, pos - 1) == schema_name.substr(fpos)
                     && (fpos == 0 || schema_name[fpos - 1] == '.');
            };
            if (!match_name())
              return false;
            TENZIR_ASSERT(!field.is_standalone_type());
            return compatible(field.type(), x.op, d);
          };
          return search(pred);
        },
        [&](const type_extractor& lhs,
            const data& d) -> catalog_lookup_result::candidate_info {
          auto result = [&] {
            if (!lhs.type) {
              auto pred = [&](auto& field) {
                const auto type = field.type();
                for (const auto& name : type.names())
                  if (name == lhs.type.name())
                    return compatible(type, x.op, d);
                return false;
              };
              return search(pred);
            }
            auto pred = [&](auto& field) {
              return congruent(field.type(), lhs.type);
            };
            return search(pred);
          }();
          return result;
        },
        [&](const auto&, const auto&) -> catalog_lookup_result::candidate_info {
          TENZIR_WARN("{} cannot process predicate: {}",
                      detail::pretty_type_name(this), x);
          return all_partitions();
        },
      };
      return caf::visit(extract_expr, x.lhs, x.rhs);
    },
    [&](caf::none_t) -> catalog_lookup_result::candidate_info {
      TENZIR_ERROR("{} received an empty expression",
                   detail::pretty_type_name(this));
      TENZIR_ASSERT(!"invalid expression");
      return all_partitions();
    },
  };
  auto result = caf::visit(f, expr);
  result.exp = expr;
  return result;
}

auto catalog_state::memusage() const -> size_t {
  size_t result = 0;
  for (const auto& [type, id_synopsis_map] : synopses_per_type) {
    for (const auto& [id, synopsis] : id_synopsis_map) {
      result += synopsis->memusage();
    }
  }
  return result;
}

void catalog_state::update_unprunable_fields(const partition_synopsis& ps) {
  for (auto const& [field, synopsis] : ps.field_synopses_)
    if (synopsis != nullptr
        && caf::holds_alternative<string_type>(field.type()))
      unprunable_fields.insert(std::string{field.name()});
  // TODO/BUG: We also need to prevent pruning for enum types,
  // which also use string literals for lookup. We must be even
  // more strict here than with string fields, because incorrectly
  // pruning string fields will only cause false positives, but
  // incorrectly pruning enum fields can actually cause false negatives.
  //
  // else if (field.type() == enumeration_type{}) {
  //   auto full_name = field.name();
  //   for (auto suffix : detail::all_suffixes(full_name, "."))
  //     unprunable_fields.insert(suffix);
  // }
}

void catalog_state::emit_metrics() const {
  TENZIR_ASSERT(accountant);
  auto num_partitions_and_events_per_schema_and_version
    = detail::stable_map<std::pair<std::string_view, uint64_t>,
                         std::pair<uint64_t, uint64_t>>{};
  for (const auto& [type, id_synopsis_map] : synopses_per_type) {
    for (const auto& [id, synopsis] : id_synopsis_map) {
      TENZIR_ASSERT(synopsis);
      auto& [num_partitions, num_events]
        = num_partitions_and_events_per_schema_and_version[std::pair{
          synopsis->schema.name(), synopsis->version}];
      num_partitions += 1;
      num_events += synopsis->events;
    }
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

auto catalog(catalog_actor::stateful_pointer<catalog_state> self,
             accountant_actor accountant) -> catalog_actor::behavior_type {
  if (self->getf(caf::local_actor::is_detached_flag))
    caf::detail::set_thread_name("tenzir.catalog");
  self->state.self = self;
  self->state.taxonomies.concepts = modules::concepts();
  if (accountant) {
    self->state.accountant = std::move(accountant);
    self->send(self->state.accountant, atom::announce_v, self->name());
    detail::weak_run_delayed_loop(self, defaults::telemetry_rate, [self] {
      self->state.emit_metrics();
    });
  }
  return {
    [self](
      atom::merge,
      std::shared_ptr<std::unordered_map<uuid, partition_synopsis_ptr>>& ps) {
      return self->state.initialize(std::move(ps));
    },
    [self](atom::merge,
           std::vector<partition_synopsis_pair>& partition_synopses) {
      return self->state.merge(std::move(partition_synopses));
    },
    [self](atom::get) -> std::vector<partition_synopsis_pair> {
      std::vector<partition_synopsis_pair> result;
      result.reserve(self->state.synopses_per_type.size());
      for (const auto& [type, id_synopsis_map] :
           self->state.synopses_per_type) {
        for (const auto& [id, synopsis] : id_synopsis_map) {
          result.push_back({id, synopsis});
        }
      }
      return result;
    },
    [self](atom::get, const expression& filter)
      -> caf::result<std::vector<partition_synopsis_pair>> {
      auto result = std::vector<partition_synopsis_pair>{};
      const auto candidates = self->state.lookup(filter);
      if (not candidates) {
        return candidates.error();
      }
      for (const auto& [schema, candidate] : candidates->candidate_infos) {
        const auto& partition_synopses
          = self->state.synopses_per_type.find(schema);
        TENZIR_ASSERT(partition_synopses
                      != self->state.synopses_per_type.end());
        for (const auto& partition : candidate.partition_infos) {
          const auto& synopsis
            = partition_synopses->second.find(partition.uuid);
          if (synopsis == partition_synopses->second.end()) {
            continue;
          }
          result.push_back({synopsis->first, synopsis->second});
        }
      }
      return result;
    },
    [self](atom::erase, uuid partition) {
      self->state.erase(partition);
      return atom::ok_v;
    },
    [self](atom::replace, const std::vector<uuid>& old_uuids,
           std::vector<partition_synopsis_pair>& new_synopses) {
      for (auto const& uuid : old_uuids)
        self->state.erase(uuid);
      return self->state.merge(std::move(new_synopses));
    },
    [self](atom::candidates, const tenzir::query_context& query_context)
      -> caf::result<catalog_lookup_result> {
      TENZIR_TRACE_SCOPE("{} {}", *self, TENZIR_ARG(query_context));
      if (not query_context.ids.empty()) {
        return caf::make_error(ec::invalid_argument, "catalog expects queries "
                                                     "not to have ids");
      }
      auto start = std::chrono::steady_clock::now();
      auto result = self->state.lookup(query_context.expr);
      if (!result) {
        return result.error();
      }
      auto total_candidate_amount = std::transform_reduce(
        result->candidate_infos.begin(), result->candidate_infos.end(),
        size_t{0}, std::plus<>{}, [](const auto& candidate) {
          return candidate.second.partition_infos.size();
        });
      auto id_str = fmt::to_string(query_context.id);
      duration runtime = std::chrono::steady_clock::now() - start;
      self->send(self->state.accountant, atom::metrics_v,
                 "catalog.lookup.runtime", runtime,
                 metrics_metadata{
                   {"query", id_str},
                   {"issuer", query_context.issuer},
                 });
      self->send(self->state.accountant, atom::metrics_v,
                 "catalog.lookup.candidates", total_candidate_amount,
                 metrics_metadata{
                   {"query", std::move(id_str)},
                   {"issuer", query_context.issuer},
                 });
      return result;
    },
    [self](atom::get, uuid uuid) -> caf::result<partition_info> {
      for (const auto& [type, synopses] : self->state.synopses_per_type) {
        if (auto it = synopses.find(uuid); it != synopses.end()) {
          return partition_info{uuid, *it->second};
        }
      }
      return caf::make_error(
        tenzir::ec::lookup_error,
        fmt::format("unable to find partition with uuid: {}", uuid));
    },
    [](atom::status, status_verbosity, duration) {
      return record{};
    },
  };
}

} // namespace tenzir
