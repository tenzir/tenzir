//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/meta_index.hpp"

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
#include "vast/query.hpp"
#include "vast/synopsis.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/system/status.hpp"
#include "vast/table_slice.hpp"
#include "vast/time.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <type_traits>

namespace vast::system {

size_t meta_index_state::memusage() const {
  size_t result = 0;
  for (const auto& [id, partition_synopsis] : synopses)
    result += partition_synopsis.memusage();
  return result;
}

void meta_index_state::erase(const uuid& partition) {
  synopses.erase(partition);
  offset_map.erase_value(partition);
}

void meta_index_state::merge(const uuid& partition, partition_synopsis&& ps) {
  synopses.emplace(partition, std::move(ps));
  offset_map.inject(ps.offset, ps.offset + ps.events, partition);
}

void meta_index_state::create_from(std::map<uuid, partition_synopsis>&& ps) {
  std::vector<std::pair<uuid, partition_synopsis>> flat_data;
  for (auto&& [uuid, synopsis] : std::move(ps)) {
    offset_map.inject(synopsis.offset, synopsis.offset + synopsis.events, uuid);
    flat_data.emplace_back(uuid, std::move(synopsis));
  }
  std::sort(flat_data.begin(), flat_data.end(),
            [](const std::pair<uuid, partition_synopsis>& lhs,
               const std::pair<uuid, partition_synopsis>& rhs) {
              return lhs.first < rhs.first;
            });
  synopses = decltype(synopses)::make_unsafe(std::move(flat_data));
}

partition_synopsis& meta_index_state::at(const uuid& partition) {
  return synopses.at(partition);
}

// A custom expression visitor that optimizes a given expression specifically
// for the meta index lookup. Currently this does only a single optimization:
// It deduplicates string lookups for the type level string synopsis.
struct pruner {
  expression operator()(caf::none_t) const {
    return expression{};
  }
  expression operator()(const conjunction& c) const {
    return conjunction{run(c)};
  }
  expression operator()(const disjunction& d) const {
    return disjunction{run(d)};
  }
  expression operator()(const negation& n) const {
    return negation{caf::visit(*this, n.expr())};
  }
  expression operator()(const predicate& p) const {
    return p;
  }

  [[nodiscard]] std::vector<expression>
  run(const std::vector<expression>& connective) const {
    std::vector<expression> result;
    detail::stable_set<std::string> memo;
    for (const auto& operand : connective) {
      const std::string* str = nullptr;
      if (const auto* pred = caf::get_if<predicate>(&operand)) {
        if (!caf::holds_alternative<meta_extractor>(pred->lhs)) {
          if (const auto* d = caf::get_if<data>(&pred->rhs)) {
            if ((str = caf::get_if<std::string>(d))) {
              if (memo.find(*str) != memo.end())
                continue;
              memo.insert(*str);
              result.emplace_back(*pred);
            }
          }
        }
      }
      if (!str)
        result.push_back(caf::visit(*this, operand));
    }
    return result;
  }
};

// Runs the `pruner` and `hoister` until the input is unchanged.
expression prune_all(expression e) {
  expression result = caf::visit(pruner{}, e);
  while (result != e) {
    std::swap(result, e);
    result = hoist(caf::visit(pruner{}, e));
  }
  return result;
}

std::vector<uuid> meta_index_state::lookup(const expression& expr) const {
  auto start = system::stopwatch::now();
  auto pruned = prune_all(expr);
  auto result = lookup_impl(pruned);
  auto delta = std::chrono::duration_cast<std::chrono::microseconds>(
    system::stopwatch::now() - start);
  VAST_DEBUG("meta index lookup found {} candidates in {} microseconds",
             result.size(), delta.count());
  VAST_TRACEPOINT(meta_index_lookup, delta.count(), result.size());
  return result;
}

std::vector<uuid> meta_index_state::lookup_impl(const expression& expr) const {
  VAST_ASSERT(!caf::holds_alternative<caf::none_t>(expr));
  // The partition UUIDs must be sorted, otherwise the invariants of the
  // inplace union and intersection algorithms are violated, leading to
  // wrong results. So all places where we return an assembled set must
  // ensure the post-condition of returning a sorted list. We currently
  // rely on `flat_map` already traversing them in the correct order, so
  // no separate sorting step is required.
  using result_type = std::vector<uuid>;
  result_type memoized_partitions;
  auto all_partitions = [&] {
    if (!memoized_partitions.empty() || synopses.empty())
      return memoized_partitions;
    memoized_partitions.reserve(synopses.size());
    std::transform(synopses.begin(), synopses.end(),
                   std::back_inserter(memoized_partitions),
                   [](auto& x) { return x.first; });
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
      // uses a qualified_record_field to determine whether the synopsis should
      // be queried.
      auto search = [&](auto match) {
        VAST_ASSERT(caf::holds_alternative<data>(x.rhs));
        const auto& rhs = caf::get<data>(x.rhs);
        result_type result;
        for (const auto& [part_id, part_syn] : synopses) {
          for (const auto& [field, syn] : part_syn.field_synopses_) {
            if (match(field)) {
              auto cleaned_type = vast::legacy_type{field.type}.attributes({});
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
              } else if (auto it = part_syn.type_synopses_.find(cleaned_type);
                         it != part_syn.type_synopses_.end() && it->second) {
                auto opt = it->second->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  VAST_TRACE("{} selects {} at predicate {}",
                             detail::pretty_type_name(this), part_id, x);
                  result.push_back(part_id);
                  break;
                }
              } else {
                // The meta index couldn't rule out this partition, so we have
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
        [&](const meta_extractor& lhs, const data& d) -> result_type {
          if (lhs.kind == meta_extractor::type) {
            // We don't have to look into the synopses for type queries, just
            // at the layout names.
            result_type result;
            for (const auto& [part_id, part_syn] : synopses) {
              for (const auto& pair : part_syn.field_synopses_) {
                // TODO: provide an overload for view of evaluate() so that
                // we can use string_view here. Fortunately type names are
                // short, so we're probably not hitting the allocator due to
                // SSO.
                auto type_name = data{pair.first.layout_name};
                if (evaluate(type_name, x.op, d)) {
                  result.push_back(part_id);
                  break;
                }
              }
            }
            VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
            return result;
          } else if (lhs.kind == meta_extractor::field) {
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
                  for (const auto& pair : synopsis.second.field_synopses_) {
                    if (const auto fqn = pair.first.fqn(); fqn.ends_with(*s))
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
        [&](const field_extractor& lhs, const data&) -> result_type {
          auto pred = [&](const auto& field) {
            return field.fqn().ends_with(lhs.field);
          };
          return search(pred);
        },
        [&](const type_extractor& lhs, const data& d) -> result_type {
          auto result = [&] {
            if (caf::holds_alternative<legacy_none_type>(lhs.type)) {
              VAST_ASSERT(!lhs.type.name().empty());
              auto pred = [&](auto& field) {
                const auto* p = &field.type;
                while (const auto* a = caf::get_if<legacy_alias_type>(p)) {
                  if (a->name() == lhs.type.name())
                    return compatible(*a, x.op, d);
                  p = &a->value_type;
                }
                if (p->name() == lhs.type.name())
                  return compatible(*p, x.op, d);
                return false;
              };
              return search(pred);
            }
            auto pred
              = [&](auto& field) { return congruent(field.type, lhs.type); };
            return search(pred);
          }();
          // Preserve compatibility with databases that were created beore
          // the #timestamp attribute was removed.
          if (lhs.type.name() == "timestamp") {
            auto pred = [](auto& field) {
              return has_attribute(field.type, "timestamp");
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

meta_index_actor::behavior_type
meta_index(meta_index_actor::stateful_pointer<meta_index_state> self) {
  self->state.self = self;
  return {
    [=](atom::merge,
        std::shared_ptr<std::map<uuid, partition_synopsis>>& ps) -> atom::ok {
      self->state.create_from(std::move(*ps));
      return atom::ok_v;
    },
    [=](atom::merge, uuid partition,
        std::shared_ptr<partition_synopsis>& synopsis) -> atom::ok {
      VAST_TRACE_SCOPE("{} {}", *self, VAST_ARG(partition));
      self->state.merge(partition, std::move(*synopsis));
      return atom::ok_v;
    },
    [=](atom::erase, uuid partition) {
      self->state.erase(partition);
      return atom::ok_v;
    },
    [=](atom::replace, uuid old_partition, uuid new_partition,
        std::shared_ptr<partition_synopsis>& synopsis) -> atom::ok {
      // There's technically no need for this assertion, at some point
      // we probably want to remove it or add a new `atom::update` handler
      // for in-place replacements.
      VAST_ASSERT(old_partition != new_partition);
      self->state.merge(new_partition, std::move(*synopsis));
      self->state.erase(old_partition);
      return atom::ok_v;
    },
    [=](atom::candidates, const vast::expression& expression,
        const vast::ids& ids) -> caf::result<std::vector<vast::uuid>> {
      VAST_TRACE_SCOPE("{} {} {}", *self, VAST_ARG(expression), VAST_ARG(ids));
      std::vector<vast::uuid> expression_candidates;
      std::vector<vast::uuid> ids_candidates;
      bool has_expression = expression != vast::expression{};
      bool has_ids = !ids.empty();
      if (!has_expression && !has_ids)
        return caf::make_error(ec::invalid_argument, "query had neither an "
                                                     "expression nor ids");
      if (has_expression) {
        expression_candidates = self->state.lookup(expression);
      }
      if (has_ids) {
        for (auto id : select(ids)) {
          const auto* x = self->state.offset_map.lookup(id);
          if (x)
            ids_candidates.push_back(*x);
        }
        std::sort(ids_candidates.begin(), ids_candidates.end());
        auto new_end
          = std::unique(ids_candidates.begin(), ids_candidates.end());
        ids_candidates.erase(new_end, ids_candidates.end());
      }
      std::vector<vast::uuid> result;
      if (has_expression && has_ids)
        std::set_intersection(expression_candidates.begin(),
                              expression_candidates.end(),
                              ids_candidates.begin(), ids_candidates.end(),
                              std::back_inserter(result));
      else if (has_expression)
        result = std::move(expression_candidates);
      else
        result = std::move(ids_candidates);
      return result;
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
