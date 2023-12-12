//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/catalog_lookup.hpp"

#include "tenzir/pipeline.hpp"
#include "tenzir/prune.hpp"

namespace tenzir {

namespace {

/// Performs a lookup against a partition synopsis, returning the corresponding
/// partition info if the expression cannot be ruled out for the partition. The
/// expression must be normalized, validated, resolved and pruned for the
/// partitions's schema.
auto lookup(const partition_synopsis& synopsis, const expression& expr)
  -> std::optional<partition_info> {
  TENZIR_ASSERT_CHEAP
  // FIXME: re-implement the lookup.
  return std::nullopt;
}

} // namespace

auto catalog_lookup_state::run() -> void {
  if (remaining_partitions.empty()) {
    if (results.empty()) {
      self->quit();
    }
    return;
  }
  // Take the top partition.
  auto partition = std::move(remaining_partitions.front());
  remaining_partitions.pop_front();
  // Bind the expression to the schema if that hasn't yet been done.
  auto bound_expr = bound_exprs.find(partition.synopsis->schema);
  if (bound_expr == bound_exprs.end()) {
    const auto resolved
      = resolve(taxonomies, query.expr, partition.synopsis->schema);
    if (not resolved) {
      self->quit(
        caf::make_error(ec::invalid_argument,
                        fmt::format("{} failed to resolve epxression {}: "
                                    "{}",
                                    *self, query.expr, resolved.error())));
      return;
    }
    auto pruned = prune(*resolved, unprunable_fields);
    bound_expr = bound_exprs.emplace_hint(
      bound_expr, partition.synopsis->schema, std::move(pruned));
  }
  // Perform the lookup.
  if (auto result = lookup(*partition.synopsis, *bound_expr)) {
    results.push_back(std::move(*result));
  }
  // Schedule the next partition if there still is one.
  if (not remaining_partitions.empty() and results.size() < cache_capacity) {
    detail::weak_run_delayed(self, duration::zero(), [this] {
      run();
    });
  }
}

auto make_catalog_lookup(
  catalog_lookup_actor::stateful_pointer<catalog_lookup_state> self,
  std::deque<partition_synopsis_pair> partitions,
  detail::heterogeneous_string_hashset unprunable_fields,
  struct taxonomies taxonomies, query_context query, uint64_t cache_capacity)
  -> catalog_lookup_actor::behavior_type {
  // Copy things into the state that we need later on.
  self->state.self = self;
  self->state.query = std::move(query);
  self->state.unprunable_fields = std::move(unprunable_fields);
  self->state.taxonomies = std::move(taxonomies);
  self->state.remaining_partitions = std::move(partitions);
  self->state.cache_capacity = std::min(1ull, cache_capacity);
  if (self->state.query.expr == caf::none) {
    // If there is no query, we can make the results available instantly.
    for (auto&& partition :
         std::exchange(self->state.remaining_partitions, {})) {
      self->state.results.emplace_back(partition_info{partition.uuid,
                                                      *partition.synopsis},
                                       trivially_true_expression());
    }
  } else {
    // Normalize and validate the query.
    auto normalized = normalize_and_validate(self->state.query.expr);
    if (not normalized) {
      self->quit(caf::make_error(
        ec::invalid_argument,
        fmt::format("{} failed to normalize and validate "
                    "epxression {}: {}",
                    *self, self->state.query.expr, normalized.error())));
      return catalog_lookup_actor::behavior_type::make_empty_behavior();
    }
    self->state.query.expr = std::move(*normalized);
  }
  // Start eagerly computing.
  detail::weak_run_delayed(self, duration::zero(), [self] {
    self->state.run();
  });
  return {
    [self](atom::get) -> caf::result<std::vector<catalog_lookup_result>> {
      if (self->state.get_rp.pending()) [[unlikely]] {
        return caf::make_error(
          ec::logic_error,
          fmt::format("{} does not support concurrent 'get' requests", *self));
      }
      if (not self->state.results.empty()) {
        detail::weak_run_delayed(self, duration::zero(), [self] {
          self->state.run();
        });
        return std::exchange(self->state.results, {});
      }
      self->state.get_rp
        = self->make_response_promise<std::vector<catalog_lookup_result>>();
      return self->state.get_rp;
    },
  };
}

// legacy_catalog_lookup_result::candidate_info
// catalog_state::lookup_impl(const expression& expr, const type& schema) const {
//   TENZIR_ASSERT(!caf::holds_alternative<caf::none_t>(expr));
//   auto synopsis_map_per_type_it = synopses_per_type.find(schema);
//   TENZIR_ASSERT(synopsis_map_per_type_it != synopses_per_type.end());
//   const auto& partition_synopses = synopsis_map_per_type_it->second;
//   // The partition UUIDs must be sorted, otherwise the invariants of the
//   // inplace union and intersection algorithms are violated, leading to
//   // wrong results. So all places where we return an assembled set must
//   // ensure the post-condition of returning a sorted list. We currently
//   // rely on `flat_map` already traversing them in the correct order, so
//   // no separate sorting step is required.
//   auto memoized_partitions = legacy_catalog_lookup_result::candidate_info{};
//   auto all_partitions = [&] {
//     if (!memoized_partitions.partition_infos.empty()
//         || partition_synopses.empty())
//       return memoized_partitions;
//     for (const auto& [partition_id, synopsis] : partition_synopses) {
//       memoized_partitions.partition_infos.emplace_back(partition_id,
//       *synopsis);
//     }
//     return memoized_partitions;
//   };
//   auto f = detail::overload{
//     [&](const conjunction& x) -> legacy_catalog_lookup_result::candidate_info {
//       TENZIR_ASSERT(!x.empty());
//       auto i = x.begin();
//       auto result = lookup_impl(*i, schema);
//       if (!result.partition_infos.empty())
//         for (++i; i != x.end(); ++i) {
//           // TODO: A conjunction means that we can restrict the lookup to the
//           // remaining candidates. This could be achived by passing the
//           `result`
//           // set to `lookup` along with the child expression.
//           auto xs = lookup_impl(*i, schema);
//           if (xs.partition_infos.empty())
//             return xs; // short-circuit
//           detail::inplace_intersect(result.partition_infos,
//           xs.partition_infos);
//           TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                        result.partition_infos.end()));
//         }
//       return result;
//     },
//     [&](const disjunction& x) -> legacy_catalog_lookup_result::candidate_info {
//       legacy_catalog_lookup_result::candidate_info result;
//       for (const auto& op : x) {
//         // TODO: A disjunction means that we can restrict the lookup to the
//         // set of partitions that are outside of the current result set.
//         auto xs = lookup_impl(op, schema);
//         if (xs.partition_infos.size() == partition_synopses.size())
//           return xs; // short-circuit
//         TENZIR_ASSERT(
//           std::is_sorted(xs.partition_infos.begin(),
//           xs.partition_infos.end()));
//         detail::inplace_unify(result.partition_infos, xs.partition_infos);
//         TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                      result.partition_infos.end()));
//       }
//       return result;
//     },
//     [&](const negation&) -> legacy_catalog_lookup_result::candidate_info {
//       // We cannot handle negations, because a synopsis may return false
//       // positives, and negating such a result may cause false
//       // negatives.
//       // TODO: The above statement seems to only apply to bloom filter
//       // synopses, but it should be possible to handle time or bool synopses.
//       return all_partitions();
//     },
//     [&](const predicate& x) -> legacy_catalog_lookup_result::candidate_info {
//       // Performs a lookup on all *matching* synopses with operator and
//       // data from the predicate of the expression. The match function
//       // uses a qualified_record_field to determine whether the synopsis
//       // should be queried.
//       auto search = [&](auto match) {
//         TENZIR_ASSERT(caf::holds_alternative<data>(x.rhs));
//         const auto& rhs = caf::get<data>(x.rhs);
//         legacy_catalog_lookup_result::candidate_info result;
//         // dont iterate through all synopses, rewrite lookup_impl to use a
//         // singular type all synopses loops -> relevant anymore? Use type as
//         // synopses key
//         for (const auto& [part_id, part_syn] : partition_synopses) {
//           for (const auto& [field, syn] : part_syn->field_synopses_) {
//             if (match(field)) {
//               // We need to prune the type's metadata here by converting it to
//               // a concrete type and back, because the type synopses are
//               // looked up independent from names and attributes.
//               auto prune = [&]<concrete_type T>(const T& x) {
//                 return type{x};
//               };
//               auto cleaned_type = caf::visit(prune, field.type());
//               // We rely on having a field -> nullptr mapping here for the
//               // fields that don't have their own synopsis.
//               if (syn) {
//                 auto opt = syn->lookup(x.op, make_view(rhs));
//                 if (!opt || *opt) {
//                   TENZIR_TRACE("{} selects {} at predicate {}",
//                                detail::pretty_type_name(this), part_id, x);
//                   result.partition_infos.emplace_back(part_id, *part_syn);
//                   break;
//                 }
//                 // The field has no dedicated synopsis. Check if there is one
//                 // for the type in general.
//               } else if (auto it = part_syn->type_synopses_.find(cleaned_type);
//                          it != part_syn->type_synopses_.end() && it->second) {
//                 auto opt = it->second->lookup(x.op, make_view(rhs));
//                 if (!opt || *opt) {
//                   TENZIR_TRACE("{} selects {} at predicate {}",
//                                detail::pretty_type_name(this), part_id, x);
//                   result.partition_infos.emplace_back(part_id, *part_syn);
//                   break;
//                 }
//               } else {
//                 // The catalog couldn't rule out this partition, so we have
//                 // to include it in the result set.
//                 result.partition_infos.emplace_back(part_id, *part_syn);
//                 break;
//               }
//             }
//           }
//         }
//         TENZIR_DEBUG("{} checked {} partitions for predicate {} and got {} "
//                      "results",
//                      detail::pretty_type_name(this), synopses_per_type.size(),
//                      x, result.partition_infos.size());
//         // Some calling paths require the result to be sorted.
//         TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                      result.partition_infos.end()));
//         return result;
//       };
//       auto extract_expr = detail::overload{
//         [&](const meta_extractor& lhs,
//             const data& d) -> legacy_catalog_lookup_result::candidate_info {
//           switch (lhs.kind) {
//             case meta_extractor::schema: {
//               // We don't have to look into the synopses for type queries, just
//               // at the schema names.
//               legacy_catalog_lookup_result::candidate_info result;
//               for (const auto& [part_id, part_syn] : partition_synopses) {
//                 for (const auto& [fqf, _] : part_syn->field_synopses_) {
//                   // TODO: provide an overload for view of evaluate() so that
//                   // we can use string_view here. Fortunately type names are
//                   // short, so we're probably not hitting the allocator due to
//                   // SSO.
//                   if (evaluate(std::string{fqf.schema_name()}, x.op, d)) {
//                     result.partition_infos.emplace_back(part_id, *part_syn);
//                     break;
//                   }
//                 }
//               }
//               TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                            result.partition_infos.end()));
//               return result;
//             }
//             case meta_extractor::schema_id: {
//               auto result = legacy_catalog_lookup_result::candidate_info{};
//               for (const auto& [part_id, part_syn] : partition_synopses) {
//                 TENZIR_ASSERT(part_syn->schema == schema);
//               }
//               if (evaluate(schema.make_fingerprint(), x.op, d)) {
//                 for (const auto& [part_id, part_syn] : partition_synopses) {
//                   result.partition_infos.emplace_back(part_id, *part_syn);
//                 }
//               }
//               TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                            result.partition_infos.end()));
//               return result;
//             }
//             case meta_extractor::import_time: {
//               legacy_catalog_lookup_result::candidate_info result;
//               for (const auto& [part_id, part_syn] : partition_synopses) {
//                 TENZIR_ASSERT(
//                   part_syn->min_import_time <= part_syn->max_import_time,
//                   "encountered empty or moved-from partition synopsis");
//                 auto ts = time_synopsis{
//                   part_syn->min_import_time,
//                   part_syn->max_import_time,
//                 };
//                 auto add = ts.lookup(x.op, caf::get<tenzir::time>(d));
//                 if (!add || *add) {
//                   result.partition_infos.emplace_back(part_id, *part_syn);
//                 }
//               }
//               TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                            result.partition_infos.end()));
//               return result;
//             }
//             case meta_extractor::internal: {
//               auto result = legacy_catalog_lookup_result::candidate_info{};
//               for (const auto& [part_id, part_syn] : partition_synopses) {
//                 auto internal = false;
//                 if (part_syn->schema) {
//                   internal =
//                   part_syn->schema.attribute("internal").has_value();
//                 }
//                 if (evaluate(internal, x.op, d)) {
//                   result.partition_infos.emplace_back(part_id, *part_syn);
//                 }
//               };
//               TENZIR_ASSERT(std::is_sorted(result.partition_infos.begin(),
//                                            result.partition_infos.end()));
//               return result;
//             }
//           }
//           TENZIR_WARN("{} cannot process meta extractor: {}",
//                       detail::pretty_type_name(this), lhs.kind);
//           return all_partitions();
//         },
//         [&](const field_extractor& lhs,
//             const data& d) -> legacy_catalog_lookup_result::candidate_info {
//           auto pred = [&](const auto& field) {
//             auto match_name = [&] {
//               auto field_name = field.field_name();
//               auto key = std::string_view{lhs.field};
//               if (field_name.length() >= key.length()) {
//                 auto pos = field_name.length() - key.length();
//                 auto sub = field_name.substr(pos);
//                 return sub == key && (pos == 0 || field_name[pos - 1] == '.');
//               }
//               auto schema_name = field.schema_name();
//               if (key.length() > schema_name.length() + 1 +
//               field_name.length())
//                 return false;
//               auto pos = key.length() - field_name.length();
//               auto second = key.substr(pos);
//               if (second != field_name)
//                 return false;
//               if (key[pos - 1] != '.')
//                 return false;
//               auto fpos = schema_name.length() - (pos - 1);
//               return key.substr(0, pos - 1) == schema_name.substr(fpos)
//                      && (fpos == 0 || schema_name[fpos - 1] == '.');
//             };
//             if (!match_name())
//               return false;
//             TENZIR_ASSERT(!field.is_standalone_type());
//             return compatible(field.type(), x.op, d);
//           };
//           return search(pred);
//         },
//         [&](const type_extractor& lhs,
//             const data& d) -> legacy_catalog_lookup_result::candidate_info {
//           auto result = [&] {
//             if (!lhs.type) {
//               auto pred = [&](auto& field) {
//                 const auto type = field.type();
//                 for (const auto& name : type.names())
//                   if (name == lhs.type.name())
//                     return compatible(type, x.op, d);
//                 return false;
//               };
//               return search(pred);
//             }
//             auto pred = [&](auto& field) {
//               return congruent(field.type(), lhs.type);
//             };
//             return search(pred);
//           }();
//           return result;
//         },
//         [&](const auto&,
//             const auto&) -> legacy_catalog_lookup_result::candidate_info {
//           TENZIR_WARN("{} cannot process predicate: {}",
//                       detail::pretty_type_name(this), x);
//           return all_partitions();
//         },
//       };
//       return caf::visit(extract_expr, x.lhs, x.rhs);
//     },
//     [&](caf::none_t) -> legacy_catalog_lookup_result::candidate_info {
//       TENZIR_ERROR("{} received an empty expression",
//                    detail::pretty_type_name(this));
//       TENZIR_ASSERT(!"invalid expression");
//       return all_partitions();
//     },
//   };
//   auto result = caf::visit(f, expr);
//   result.exp = expr;
//   return result;
// }
//

} // namespace tenzir
