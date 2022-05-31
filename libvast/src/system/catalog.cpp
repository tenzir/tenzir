//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/catalog.hpp"

#include "vast/data.hpp"
#include "vast/detail/fill_status_map.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/set_operations.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/tracepoint.hpp"
#include "vast/expression.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/prune.hpp"
#include "vast/query.hpp"
#include "vast/synopsis.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/report.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"
#include "vast/time.hpp"

#include <caf/binary_serializer.hpp>

#include <type_traits>

namespace vast::system {

void catalog_state::update_unprunable_fields(const partition_synopsis& ps) {
  for (auto const& [field, synopsis] : ps.field_synopses_)
    if (synopsis != nullptr && field.type() == string_type{})
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

size_t catalog_state::memusage() const {
  size_t result = 0;
  for (const auto& [id, partition_synopsis] : synopses)
    result += partition_synopsis->memusage();
  return result;
}

void catalog_state::erase(const uuid& partition) {
  synopses.erase(partition);
  offset_map.erase_value(partition);
}

void catalog_state::merge(const uuid& partition, partition_synopsis_ptr ps) {
  offset_map.inject(ps->offset, ps->offset + ps->events, partition);
  update_unprunable_fields(*ps);
  synopses.emplace(partition, std::move(ps));
}

void catalog_state::create_from(std::map<uuid, partition_synopsis_ptr>&& ps) {
  std::vector<std::pair<uuid, partition_synopsis_ptr>> flat_data;
  for (auto&& [uuid, synopsis] : std::move(ps)) {
    VAST_ASSERT(synopsis->get_reference_count() == 1ull);
    offset_map.inject(synopsis->offset, synopsis->offset + synopsis->events,
                      uuid);
    flat_data.emplace_back(uuid, std::move(synopsis));
  }
  std::sort(flat_data.begin(), flat_data.end(),
            [](const std::pair<uuid, partition_synopsis_ptr>& lhs,
               const std::pair<uuid, partition_synopsis_ptr>& rhs) {
              return lhs.first < rhs.first;
            });
  synopses = decltype(synopses)::make_unsafe(std::move(flat_data));
  for (auto const& [_, synopsis] : synopses)
    update_unprunable_fields(*synopsis);
}

partition_synopsis_ptr& catalog_state::at(const uuid& partition) {
  return synopses.at(partition);
}

catalog_result catalog_state::lookup(const expression& expr) const {
  auto start = system::stopwatch::now();
  auto pruned = prune(expr, unprunable_fields);
  auto result = lookup_impl(pruned);
  auto delta = std::chrono::duration_cast<std::chrono::microseconds>(
    system::stopwatch::now() - start);
  VAST_DEBUG("catalog lookup found {} candidates in {} microseconds",
             result.size(), delta.count());
  VAST_TRACEPOINT(catalog_lookup, delta.count(), result.size());
  // TODO: This is correct because every exact result can be regarded as a
  // probabilistic result with zero false positives, but it is a
  // pessimization.
  // We should analyze the query here to see whether it's probabilistic or
  // exact. An exact query consists of a disjunction of exact synopses or
  // an explicit set of ids.
  return catalog_result{catalog_result::probabilistic, std::move(result)};
}

std::vector<uuid> catalog_state::lookup_impl(const expression& expr) const {
  VAST_ASSERT(!caf::holds_alternative<caf::none_t>(expr));
  // The partition UUIDs must be sorted, otherwise the invariants of the
  // inplace union and intersection algorithms are violated, leading to
  // wrong results. So all places where we return an assembled set must
  // ensure the post-condition of returning a sorted list. We currently
  // rely on `flat_map` already traversing them in the correct order, so
  // no separate sorting step is required.
  using result_type = std::vector<uuid>;
  result_type memoized_partitions = {};
  auto all_partitions = [&] {
    if (!memoized_partitions.empty() || synopses.empty())
      return memoized_partitions;
    memoized_partitions.reserve(synopses.size());
    std::transform(synopses.begin(), synopses.end(),
                   std::back_inserter(memoized_partitions), [](auto& x) {
                     return x.first;
                   });
    std::sort(memoized_partitions.begin(), memoized_partitions.end());
    return memoized_partitions;
  };
  auto f = detail::overload{
    [&](const conjunction& x) -> result_type {
      VAST_ASSERT(!x.empty());
      auto i = x.begin();
      auto result = lookup_impl(*i);
      if (!result.empty())
        for (++i; i != x.end(); ++i) {
          // TODO: A conjunction means that we can restrict the lookup to the
          // remaining candidates. This could be achived by passing the `result`
          // set to `lookup` along with the child expression.
          auto xs = lookup_impl(*i);
          if (xs.empty())
            return xs; // short-circuit
          detail::inplace_intersect(result, xs);
          VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
        }
      return result;
    },
    [&](const disjunction& x) -> result_type {
      result_type result;
      auto string_synopsis_memo = detail::stable_set<std::string>{};
      for (const auto& op : x) {
        // TODO: A disjunction means that we can restrict the lookup to the
        // set of partitions that are outside of the current result set.
        auto xs = lookup_impl(op);
        VAST_ASSERT(std::is_sorted(xs.begin(), xs.end()));
        if (xs.size() == synopses.size())
          return xs; // short-circuit
        detail::inplace_unify(result, xs);
        VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
      }
      return result;
    },
    [&](const negation&) -> result_type {
      // We cannot handle negations, because a synopsis may return false
      // positives, and negating such a result may cause false
      // negatives.
      // TODO: The above statement seems to only apply to bloom filter
      // synopses, but it should be possible to handle time or bool synopses.
      return all_partitions();
    },
    [&](const predicate& x) -> result_type {
      // Performs a lookup on all *matching* synopses with operator and
      // data from the predicate of the expression. The match function
      // uses a qualified_record_field to determine whether the synopsis
      // should be queried.
      auto search = [&](auto match) {
        VAST_ASSERT(caf::holds_alternative<data>(x.rhs));
        const auto& rhs = caf::get<data>(x.rhs);
        result_type result;
        for (const auto& [part_id, part_syn] : synopses) {
          for (const auto& [field, syn] : part_syn->field_synopses_) {
            if (match(field)) {
              // We need to prune the type's metadata here by converting it to a
              // concrete type and back, because the type synopses are looked up
              // independent from names and attributes.
              auto prune = [&]<concrete_type T>(const T& x) {
                return type{x};
              };
              auto cleaned_type = caf::visit(prune, field.type());
              // We rely on having a field -> nullptr mapping here for the
              // fields that don't have their own synopsis.
              if (syn) {
                auto opt = syn->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  VAST_TRACE("{} selects {} at predicate {}",
                             detail::pretty_type_name(this), part_id, x);
                  result.push_back(part_id);
                  break;
                }
                // The field has no dedicated synopsis. Check if there is one
                // for the type in general.
              } else if (auto it = part_syn->type_synopses_.find(cleaned_type);
                         it != part_syn->type_synopses_.end() && it->second) {
                auto opt = it->second->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  VAST_TRACE("{} selects {} at predicate {}",
                             detail::pretty_type_name(this), part_id, x);
                  result.push_back(part_id);
                  break;
                }
              } else {
                // The catalog couldn't rule out this partition, so we have
                // to include it in the result set.
                result.push_back(part_id);
                break;
              }
            }
          }
        }
        VAST_DEBUG(
          "{} checked {} partitions for predicate {} and got {} results",
          detail::pretty_type_name(this), synopses.size(), x, result.size());
        // Some calling paths require the result to be sorted.
        VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
        return result;
      };
      auto extract_expr = detail::overload{
        [&](const selector& lhs, const data& d) -> result_type {
          if (lhs.kind == selector::type) {
            // We don't have to look into the synopses for type queries, just
            // at the layout names.
            result_type result;
            for (const auto& [part_id, part_syn] : synopses) {
              for (const auto& [fqf, _] : part_syn->field_synopses_) {
                // TODO: provide an overload for view of evaluate() so that
                // we can use string_view here. Fortunately type names are
                // short, so we're probably not hitting the allocator due to
                // SSO.
                if (evaluate(std::string{fqf.layout_name()}, x.op, d)) {
                  result.push_back(part_id);
                  break;
                }
              }
            }
            VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
            return result;
          } else if (lhs.kind == selector::import_time) {
            result_type result;
            for (const auto& [part_id, part_syn] : synopses) {
              VAST_ASSERT(part_syn->min_import_time
                            <= part_syn->max_import_time,
                          "encountered empty or moved-from partition synopsis");
              auto ts = time_synopsis{
                part_syn->min_import_time,
                part_syn->max_import_time,
              };
              auto add = ts.lookup(x.op, caf::get<vast::time>(d));
              if (!add || *add)
                result.push_back(part_id);
            }
            VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
            return result;
          } else if (lhs.kind == selector::field) {
            // We don't have to look into the synopses for type queries, just
            // at the layout names.
            result_type result;
            const auto* s = caf::get_if<std::string>(&d);
            if (!s) {
              VAST_WARN("#field meta queries only support string "
                        "comparisons");
            } else {
              for (const auto& synopsis : synopses) {
                // Compare the desired field name with each field in the
                // partition.
                auto matching = [&] {
                  for (const auto& [field, _] :
                       synopsis.second->field_synopses_) {
                    VAST_ASSERT(!field.is_standalone_type());
                    auto rt = record_type{{field.field_name(), field.type()}};
                    for ([[maybe_unused]] const auto& offset :
                         rt.resolve_key_suffix(*s, field.layout_name()))
                      return true;
                  }
                  return false;
                }();
                // Only insert the partition if both sides are equal, i.e. the
                // operator is "positive" and matching is true, or both are
                // negative.
                if (!is_negated(x.op) == matching)
                  result.push_back(synopsis.first);
              }
            }
            VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
            return result;
          }
          VAST_WARN("{} cannot process attribute extractor: {}",
                    detail::pretty_type_name(this), lhs.kind);
          return all_partitions();
        },
        [&](const extractor& lhs, const data& d) -> result_type {
          auto pred = [&](const auto& field) {
            if (!compatible(field.type(), x.op, d))
              return false;
            VAST_ASSERT(!field.is_standalone_type());
            auto rt = record_type{{field.field_name(), field.type()}};
            for ([[maybe_unused]] const auto& offset :
                 rt.resolve_key_suffix(lhs.value, field.layout_name()))
              return true;
            return false;
          };
          return search(pred);
        },
        [&](const type_extractor& lhs, const data& d) -> result_type {
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
          // Preserve compatibility with databases that were created beore
          // the #timestamp attribute was removed.
          if (lhs.type.name() == "timestamp") {
            auto pred = [](auto& field) {
              const auto type = field.type();
              return type.attribute("timestamp").has_value();
            };
            detail::inplace_unify(result, search(pred));
          }
          return result;
        },
        [&](const auto&, const auto&) -> result_type {
          VAST_WARN("{} cannot process predicate: {}",
                    detail::pretty_type_name(this), x);
          return all_partitions();
        },
      };
      return caf::visit(extract_expr, x.lhs, x.rhs);
    },
    [&](caf::none_t) -> result_type {
      VAST_ERROR("{} received an empty expression",
                 detail::pretty_type_name(this));
      VAST_ASSERT(!"invalid expression");
      return all_partitions();
    },
  };
  return caf::visit(f, expr);
}

catalog_actor::behavior_type
catalog(catalog_actor::stateful_pointer<catalog_state> self,
        accountant_actor accountant) {
  self->state.self = self;
  self->state.accountant = std::move(accountant);
  self->send(self->state.accountant, atom::announce_v, self->name());
  return {
    [=](
      atom::merge,
      std::shared_ptr<std::map<uuid, partition_synopsis_ptr>>& ps) -> atom::ok {
      self->state.create_from(std::move(*ps));
      return atom::ok_v;
    },
    [=](atom::merge, uuid partition,
        partition_synopsis_ptr& synopsis) -> atom::ok {
      VAST_TRACE_SCOPE("{} {}", *self, VAST_ARG(partition));
      self->state.merge(partition, std::move(synopsis));
      return atom::ok_v;
    },
    [=](atom::get) -> std::vector<partition_synopsis_pair> {
      std::vector<partition_synopsis_pair> result;
      result.reserve(self->state.synopses.size());
      for (const auto& synopsis : self->state.synopses)
        result.push_back({synopsis.first, synopsis.second});
      return result;
    },
    [=](atom::erase, uuid partition) {
      self->state.erase(partition);
      return atom::ok_v;
    },
    [=](atom::replace, uuid old_partition, uuid new_partition,
        partition_synopsis_ptr& synopsis) -> atom::ok {
      // There's technically no need for this assertion, at some point
      // we probably want to remove it or add a new `atom::update` handler
      // for in-place replacements.
      VAST_ASSERT(old_partition != new_partition);
      self->state.merge(new_partition, std::move(synopsis));
      self->state.erase(old_partition);
      return atom::ok_v;
    },
    [=](atom::candidates, vast::uuid lookup_id,
        const vast::expression& expr) -> caf::result<catalog_result> {
      auto start = std::chrono::steady_clock::now();
      auto result = self->state.lookup(expr);
      duration runtime = std::chrono::steady_clock::now() - start;
      auto id_str = fmt::to_string(lookup_id);
      self->send(self->state.accountant, "catalog.lookup.runtime", runtime,
                 metrics_metadata{{"query", id_str}});
      self->send(self->state.accountant, "catalog.lookup.candidates",
                 result.partitions.size(),
                 metrics_metadata{{"query", std::move(id_str)}});
      return result;
    },
    [=](atom::candidates,
        const vast::query& query) -> caf::result<catalog_result> {
      VAST_TRACE_SCOPE("{} {}", *self, VAST_ARG(query));
      catalog_result expression_candidates;
      std::vector<vast::uuid> ids_candidates;
      bool has_expression = query.expr != vast::expression{};
      bool has_ids = !query.ids.empty();
      if (!has_expression && !has_ids)
        return caf::make_error(ec::invalid_argument, "query had neither an "
                                                     "expression nor ids");
      auto start = std::chrono::steady_clock::now();
      if (has_expression) {
        expression_candidates = self->state.lookup(query.expr);
      }
      if (has_ids) {
        for (auto id : select(query.ids)) {
          const auto* x = self->state.offset_map.lookup(id);
          if (x)
            ids_candidates.push_back(*x);
        }
        std::sort(ids_candidates.begin(), ids_candidates.end());
        auto new_end
          = std::unique(ids_candidates.begin(), ids_candidates.end());
        ids_candidates.erase(new_end, ids_candidates.end());
      }
      std::vector<vast::uuid> result_candidates;
      if (has_expression && has_ids)
        std::set_intersection(expression_candidates.partitions.begin(),
                              expression_candidates.partitions.end(),
                              ids_candidates.begin(), ids_candidates.end(),
                              std::back_inserter(result_candidates));
      else if (has_expression)
        result_candidates = std::move(expression_candidates.partitions);
      else
        result_candidates = std::move(ids_candidates);
      duration runtime = std::chrono::steady_clock::now() - start;
      auto id_str = fmt::to_string(query.id);
      self->send(self->state.accountant, "catalog.lookup.runtime", runtime,
                 metrics_metadata{{"query", id_str}});
      self->send(self->state.accountant, "catalog.lookup.candidates",
                 result_candidates.size(),
                 metrics_metadata{{"query", std::move(id_str)}});

      return catalog_result{catalog_result::probabilistic,
                            std::move(result_candidates)};
    },
    [=](atom::status, status_verbosity v) {
      record result;
      result["memory-usage"] = count{self->state.memusage()};
      result["num-partitions"] = count{self->state.synopses.size()};
      if (v >= status_verbosity::debug)
        detail::fill_status_map(result, self);
      return result;
    },
  };
}

} // namespace vast::system
