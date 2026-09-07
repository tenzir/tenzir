//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The candidate-evaluation machinery of the catalog, and the worker actors
// that run it. Evaluation is pure over a snapshot of the partition synopses:
// it holds no reference to catalog state, so a pool of workers can serve
// lookups while the catalog's own thread does bookkeeping -- including the
// blocking writes of the storage policy's state files.

#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/set_operations.hpp"
#include "tenzir/detail/tracepoint.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/expression_visitors.hpp"
#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/time_synopsis.hpp"

#include <caf/make_copy_on_write.hpp>

#include <chrono>
#include <filesystem>
#include <numeric>
#include <unordered_set>

namespace tenzir {

namespace {

using stopwatch = std::chrono::steady_clock;

auto contains_metadata(const expression& expr) -> bool {
  return match(
    expr,
    [](caf::none_t) {
      return false;
    },
    [](const predicate& pred) {
      return is<meta_extractor>(pred.lhs) or is<meta_extractor>(pred.rhs);
    },
    [](const conjunction& expressions) {
      return std::ranges::any_of(expressions, [](const auto& expression) {
        return contains_metadata(expression);
      });
    },
    [](const disjunction& expressions) {
      return std::ranges::any_of(expressions, [](const auto& expression) {
        return contains_metadata(expression);
      });
    },
    [](const negation& expression) {
      return contains_metadata(expression.expr());
    });
}

auto finalize_lookup(catalog_lookup_result&& candidates,
                     stopwatch::time_point start) -> catalog_lookup_result {
  // Sort each schema's partitions by recency and gather statistics.
  auto num_candidate_partitions = size_t{0};
  auto num_candidate_events = size_t{0};
  for (auto& [type, per_schema] : candidates.candidate_infos) {
    std::sort(per_schema.partition_infos.begin(),
              per_schema.partition_infos.end(),
              [](const partition_info& lhs, const partition_info& rhs) {
                return lhs.max_import_time > rhs.max_import_time;
              });
    num_candidate_partitions += per_schema.partition_infos.size();
    num_candidate_events
      += std::transform_reduce(per_schema.partition_infos.begin(),
                               per_schema.partition_infos.end(), size_t{0},
                               std::plus<>{}, [](const auto& partition) {
                                 return partition.events;
                               });
  }
  auto delta = std::chrono::duration_cast<std::chrono::microseconds>(
    stopwatch::now() - start);
  TENZIR_INFO("catalog found {} candidate partitions ({} events) in "
              "{} microseconds",
              num_candidate_partitions, num_candidate_events, delta.count());
  TENZIR_TRACEPOINT(catalog_lookup, delta.count(), num_candidate_partitions);
  return std::move(candidates);
}

} // namespace

auto catalog_lookup_engine::ensure_sketches_loaded(
  const uuid& id, const partition_synopsis_ptr& resident) -> size_t {
  if (sketches.budget() == 0) {
    return 0;
  }
  if (sketches.peek(id)) {
    return 0; // already loaded
  }
  // The sketches live in the partition's `.mdx`; we can only mmap a local file,
  // so remote stores (e.g. s3://) fall back to the conservative behavior.
  constexpr auto prefix = std::string_view{"file://"};
  const auto& url = resident->sketches_file.url;
  if (not url.starts_with(prefix)) {
    return 0;
  }
  const auto path = std::filesystem::path{url.substr(prefix.size())};
  auto chunk = chunk::mmap(path);
  if (not chunk) {
    TENZIR_DEBUG("{} could not mmap sketches for partition {} at {}: {}",
                 "catalog-lookup", id, path, chunk.error());
    return 0;
  }
  auto synopsis_fb
    = tenzir::flatbuffer<fbs::PartitionSynopsis>::make(std::move(*chunk));
  if (not synopsis_fb) {
    TENZIR_DEBUG("{} could not read sketches for partition {}: {}",
                 "catalog-lookup", id, synopsis_fb.error());
    return 0;
  }
  if ((*synopsis_fb)->partition_synopsis_type()
      != fbs::partition_synopsis::PartitionSynopsis::legacy) {
    return 0;
  }
  auto loaded = caf::make_copy_on_write<partition_synopsis>();
  // Load fully, i.e. including the deferred Bloom-filter sketches.
  if (auto error = unpack(*(*synopsis_fb)->partition_synopsis_as_legacy(),
                          loaded.unshared(), /*lazy_sketches=*/false);
      error.valid()) {
    TENZIR_DEBUG("{} could not unpack sketches for partition {}: {}",
                 "catalog-lookup", id, error);
    return 0;
  }
  // Return the bytes actually cached: `put` refuses an entry larger than the
  // whole budget, and the caller must not spend its query budget on a sketch
  // that wasn't cached (it would stop loading later, smaller candidates).
  return sketches.put(id, std::move(loaded));
}

auto catalog_lookup_engine::lookup(expression expr,
                                   const universe& synopses_per_type)
  -> caf::expected<catalog_lookup_result> {
  auto start = stopwatch::now();
  if (expr == caf::none) {
    expr = trivially_true_expression();
  }
  auto normalized = normalize_and_validate(expr);
  if (not normalized) {
    return caf::make_error(ec::invalid_argument,
                           fmt::format("{} failed to normalize and validate "
                                       "epxression {}: {}",
                                       "catalog-lookup", expr,
                                       normalized.error()));
  }
  // Short-circuit a match-everything lookup. The answer is every partition, so
  // there is nothing to prune and no reason to pay for the machinery that
  // would arrive at that conclusion: no taxonomy resolution per schema, no
  // per-partition predicate evaluation, and none of the string construction
  // the `meta_extractor::schema` branch does for each one. Rebuild and
  // compaction issue exactly this expression on every run, over every
  // partition in the database.
  if (*normalized == trivially_true_expression()) {
    auto total_candidates = catalog_lookup_result{};
    for (const auto& [type, partition_synopses] : synopses_per_type) {
      if (partition_synopses->empty()) {
        continue;
      }
      auto& candidates = total_candidates.candidate_infos[type];
      candidates.exp = trivially_true_expression();
      candidates.partition_infos.reserve(partition_synopses->size());
      for (const auto& [part_id, part_syn] : *partition_synopses) {
        candidates.partition_infos.emplace_back(part_id, *part_syn);
      }
    }
    return finalize_lookup(std::move(total_candidates), start);
  }
  // Resolve the expression once per schema; reused across both phases below.
  auto resolved_per_type = std::vector<std::pair<type, expression>>{};
  resolved_per_type.reserve(synopses_per_type.size());
  for (const auto& [type, _] : synopses_per_type) {
    auto resolved = resolve(taxonomies, *normalized, type);
    if (not resolved) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} failed to resolve epxression {}: "
                                         "{}",
                                         "catalog-lookup", expr,
                                         resolved.error()));
    }
    resolved_per_type.emplace_back(type, std::move(*resolved));
  }
  // Phase 1: prune using the resident synopses. Deferred Bloom-filter sketches
  // are treated conservatively (their partitions are kept as candidates);
  // `deferred_per_type` collects, per schema, the ids of partitions that were
  // kept only because such a sketch could prune them if loaded.
  auto deferred_per_type = std::unordered_map<type, std::unordered_set<uuid>>{};
  auto total_candidates = catalog_lookup_result{};
  for (const auto& [type, resolved] : resolved_per_type) {
    auto& deferred = deferred_per_type[type];
    auto candidates_per_type
      = lookup_impl(resolved, type, *synopses_per_type.at(type), deferred);
    if (candidates_per_type.partition_infos.empty()) {
      continue;
    }
    total_candidates.candidate_infos[type] = std::move(candidates_per_type);
  }
  // Phase 2: prune on the go. For each candidate that was kept only because of
  // a deferred Bloom filter, load its sketches and re-evaluate that single
  // partition, dropping it if its now-visible Bloom filter rules it out. Only
  // one partition's sketches need to be resident at a time, so pruning is not
  // limited by the cache budget (which only governs how many sketches stay
  // warm for later queries); the candidate set is already narrowed by the
  // cheap time/min-max pruning of phase 1. Restricting to `deferred` ids avoids
  // loading sketches for candidates a Bloom filter cannot prune (e.g. those
  // matched only by a `#schema` or other branch of a disjunction).
  if (sketches.budget() > 0) {
    for (const auto& [type, resolved] : resolved_per_type) {
      auto candidate_it = total_candidates.candidate_infos.find(type);
      if (candidate_it == total_candidates.candidate_infos.end()) {
        continue;
      }
      const auto& deferred = deferred_per_type[type];
      if (deferred.empty()) {
        continue;
      }
      const auto& partition_synopses = *synopses_per_type.at(type);
      auto& partition_infos = candidate_it->second.partition_infos;
      auto kept = std::vector<partition_info>{};
      kept.reserve(partition_infos.size());
      for (auto& info : partition_infos) {
        const auto resident = partition_synopses.find(info.uuid);
        // Only candidates kept because of a deferred Bloom filter are worth
        // loading; everything else stays as-is.
        if (not deferred.contains(info.uuid)
            or resident == partition_synopses.end()) {
          kept.push_back(std::move(info));
          continue;
        }
        ensure_sketches_loaded(info.uuid, resident->second);
        // If the sketches could not be loaded (remote/oversized/missing), keep
        // the partition as a conservative candidate.
        if (not sketches.peek(info.uuid)) {
          kept.push_back(std::move(info));
          continue;
        }
        // Re-evaluate this single partition; `lookup_impl` picks up its loaded
        // sketches via the cache. The throwaway deferred set is unused here.
        auto single = detail::flat_map<uuid, partition_synopsis_ptr>{};
        single[resident->first] = resident->second;
        auto ignored = std::unordered_set<uuid>{};
        if (not lookup_impl(resolved, type, single, ignored)
                  .partition_infos.empty()) {
          kept.push_back(std::move(info));
        }
      }
      partition_infos = std::move(kept);
    }
    // Drop schemas whose candidates were all pruned, matching phase 1's
    // "skip empty" contract.
    std::erase_if(total_candidates.candidate_infos, [](const auto& entry) {
      return entry.second.partition_infos.empty();
    });
  }
  return finalize_lookup(std::move(total_candidates), start);
}

auto catalog_lookup_engine::lookup_impl(
  const expression& expr, const type& schema,
  const detail::flat_map<uuid, partition_synopsis_ptr>& partition_synopses,
  std::unordered_set<uuid>& deferred_sketch_partitions) const
  -> catalog_lookup_result::candidate_info {
  TENZIR_ASSERT(not is<caf::none_t>(expr));
  // The partition UUIDs must be sorted, otherwise the invariants of the
  // inplace set algorithms are violated, leading to wrong results. So all
  // places where we return an assembled set must
  // ensure the post-condition of returning a sorted list. We currently
  // rely on `flat_map` already traversing them in the correct order, so
  // no separate sorting step is required.
  auto memoized_partitions = catalog_lookup_result::candidate_info{};
  auto all_partitions = [&] {
    if (not memoized_partitions.partition_infos.empty()
        or partition_synopses.empty()) {
      return memoized_partitions;
    }
    for (const auto& [partition_id, synopsis] : partition_synopses) {
      memoized_partitions.partition_infos.emplace_back(partition_id, *synopsis);
    }
    return memoized_partitions;
  };
  using synopsis_map = detail::flat_map<uuid, partition_synopsis_ptr>;
  using candidate_info = catalog_lookup_result::candidate_info;
  auto narrow_to = [&](const candidate_info& candidates) {
    auto entries = typename synopsis_map::vector_type{};
    entries.reserve(candidates.partition_infos.size());
    for (const auto& candidate : candidates.partition_infos) {
      const auto it = partition_synopses.find(candidate.uuid);
      TENZIR_ASSERT(it != partition_synopses.end());
      entries.push_back(*it);
    }
    return synopsis_map::make_unsafe(std::move(entries));
  };
  auto exclude = [&](const candidate_info& candidates) {
    TENZIR_ASSERT(candidates.partition_infos.size()
                  <= partition_synopses.size());
    auto entries = typename synopsis_map::vector_type{};
    entries.reserve(partition_synopses.size()
                    - candidates.partition_infos.size());
    auto candidate = candidates.partition_infos.begin();
    for (const auto& entry : partition_synopses) {
      if (candidate != candidates.partition_infos.end()
          and candidate->uuid == entry.first) {
        ++candidate;
        continue;
      }
      entries.push_back(entry);
    }
    TENZIR_ASSERT(candidate == candidates.partition_infos.end());
    return synopsis_map::make_unsafe(std::move(entries));
  };
  auto f = detail::overload{
    [&](const conjunction& x) -> catalog_lookup_result::candidate_info {
      TENZIR_ASSERT(not x.empty());
      auto result = catalog_lookup_result::candidate_info{};
      auto initialized = false;
      for (const auto metadata : {true, false}) {
        for (const auto& op : x) {
          if (contains_metadata(op) != metadata) {
            continue;
          }
          if (not initialized) {
            result = lookup_impl(op, schema, partition_synopses,
                                 deferred_sketch_partitions);
            initialized = true;
          } else {
            auto remaining = narrow_to(result);
            result
              = lookup_impl(op, schema, remaining, deferred_sketch_partitions);
          }
          if (result.partition_infos.empty()) {
            return result; // short-circuit
          }
        }
      }
      return result;
    },
    [&](const disjunction& x) -> catalog_lookup_result::candidate_info {
      catalog_lookup_result::candidate_info result;
      for (const auto metadata : {true, false}) {
        for (const auto& op : x) {
          if (contains_metadata(op) != metadata) {
            continue;
          }
          auto xs = catalog_lookup_result::candidate_info{};
          if (result.partition_infos.empty()) {
            xs = lookup_impl(op, schema, partition_synopses,
                             deferred_sketch_partitions);
          } else {
            auto remaining = exclude(result);
            if (remaining.empty()) {
              return result;
            }
            xs = lookup_impl(op, schema, remaining, deferred_sketch_partitions);
          }
          TENZIR_ASSERT_EXPENSIVE(std::is_sorted(xs.partition_infos.begin(),
                                                 xs.partition_infos.end()));
          detail::inplace_unify(result.partition_infos, xs.partition_infos);
          TENZIR_ASSERT_EXPENSIVE(std::is_sorted(result.partition_infos.begin(),
                                                 result.partition_infos.end()));
          if (result.partition_infos.size() == partition_synopses.size()) {
            return result; // short-circuit
          }
        }
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
        TENZIR_ASSERT(is<data>(x.rhs));
        const auto& rhs = as<data>(x.rhs);
        catalog_lookup_result::candidate_info result;
        auto matching_fields = std::vector<qualified_record_field>{};
        const auto* schema_fields = try_as<record_type>(&schema);
        const auto fields_resolved
          = schema_fields != nullptr and not schema.name().empty();
        if (fields_resolved) {
          for (const auto& leaf : schema_fields->leaves()) {
            auto field = qualified_record_field{schema, leaf.index};
            if (match(field)) {
              matching_fields.push_back(std::move(field));
            }
          }
        }
        for (const auto& [part_id, part_syn] : partition_synopses) {
          // Prefer an on-demand-loaded synopsis (with Bloom-filter sketches)
          // when one is cached; otherwise use the resident synopsis, whose
          // deferred sketches are null.
          const auto loaded = sketches.peek(part_id);
          const auto& effective = loaded ? loaded : part_syn;
          auto may_contain = [&](const qualified_record_field& field,
                                 const synopsis_ptr& syn) {
            // We need to prune the type's metadata here by converting it to a
            // concrete type and back, because the type synopses are looked up
            // independent from names and attributes.
            auto prune = [&]<concrete_type T>(const T& x) {
              return type{x};
            };
            auto cleaned_type = tenzir::match(field.type(), prune);
            if (syn) {
              auto opt = syn->lookup(x.op, make_view(rhs));
              return not opt or *opt;
            }
            // The field has no dedicated synopsis. Check if there is one for
            // the type in general.
            if (auto it = effective->type_synopses_.find(cleaned_type);
                it != effective->type_synopses_.end() and it->second) {
              auto opt = it->second->lookup(x.op, make_view(rhs));
              return not opt or *opt;
            }
            // The catalog couldn't rule out this partition, so we have to
            // include it in the result set. If the missing synopsis is a
            // deferred Bloom filter, record that loading it could prune
            // further -- but only if the Bloom filter could actually answer
            // this predicate. `bloom_filter_synopsis::lookup` only hashes
            // literal values of the field type: it prunes `equal` against a
            // literal and `in` against a list of literals. For anything else
            // (`!=`, ranges, patterns, subnets/patterns inside an `in` list,
            // type mismatches) it returns nullopt or silently skips the
            // element, so loading the sketch could not prune -- or worse,
            // could prune a partition exact evaluation would keep.
            if (not loaded) {
              // True iff `value` is a literal a Bloom filter on this field type
              // can hash (string -> string, IP -> ip).
              const auto is_bloom_literal = [&](const data& value) {
                return tenzir::match(
                  field.type(), [&]<concrete_type T>(const T&) {
                    if constexpr (std::is_same_v<T, string_type>) {
                      return is<std::string>(value);
                    } else if constexpr (std::is_same_v<T, ip_type>) {
                      return is<ip>(value);
                    } else {
                      return false;
                    }
                  });
              };
              auto bloom_prunable = false;
              if (x.op == relational_operator::equal) {
                bloom_prunable = is_bloom_literal(rhs);
              } else if (x.op == relational_operator::in) {
                if (const auto* xs = try_as<list>(&rhs)) {
                  bloom_prunable = std::ranges::all_of(*xs, is_bloom_literal);
                }
              }
              if (bloom_prunable) {
                deferred_sketch_partitions.insert(part_id);
              }
            }
            return true;
          };
          auto selected = false;
          if (fields_resolved) {
            for (const auto& field : matching_fields) {
              const auto syn = effective->field_synopses_.find(field);
              if (syn != effective->field_synopses_.end()
                  and may_contain(field, syn->second)) {
                selected = true;
                break;
              }
            }
          } else {
            // Partition v0 synopses have no schema and may be heterogeneous,
            // so resolve their matching fields separately.
            for (const auto& [field, syn] : effective->field_synopses_) {
              if (match(field) and may_contain(field, syn)) {
                selected = true;
                break;
              }
            }
          }
          if (selected) {
            TENZIR_TRACE("{} selects {} at predicate {}",
                         detail::pretty_type_name(this), part_id, x);
            result.partition_infos.emplace_back(part_id, *effective);
          }
        }
        TENZIR_DEBUG("{} checked {} partitions for predicate {} and got {} "
                     "results",
                     detail::pretty_type_name(this), partition_synopses.size(),
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
              if (schema and not schema.name().empty()) {
                if (evaluate(std::string{schema.name()}, x.op, d)) {
                  result = all_partitions();
                }
                return result;
              }
              // Partition v0 synopses have no schema, so recover their names
              // from their qualified fields instead.
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
#if TENZIR_ENABLE_ASSERTIONS
              for (const auto& [_, part_syn] : partition_synopses) {
                TENZIR_ASSERT_EXPENSIVE(part_syn->schema == schema);
              }
#endif
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
                auto add = ts.lookup(x.op, as<tenzir::time>(d));
                if (not add or *add) {
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
                return sub == key and (pos == 0 or field_name[pos - 1] == '.');
              }
              auto schema_name = field.schema_name();
              if (key.length()
                  > schema_name.length() + 1 + field_name.length()) {
                return false;
              }
              auto pos = key.length() - field_name.length();
              auto second = key.substr(pos);
              if (second != field_name) {
                return false;
              }
              if (key[pos - 1] != '.') {
                return false;
              }
              auto fpos = schema_name.length() - (pos - 1);
              return key.substr(0, pos - 1) == schema_name.substr(fpos)
                     and (fpos == 0 or schema_name[fpos - 1] == '.');
            };
            if (not match_name()) {
              return false;
            }
            TENZIR_ASSERT(not field.is_standalone_type());
            return compatible(field.type(), x.op, d);
          };
          return search(pred);
        },
        [&](const type_extractor& lhs,
            const data& d) -> catalog_lookup_result::candidate_info {
          auto result = [&] {
            if (not lhs.type) {
              auto pred = [&](auto& field) {
                const auto& type = field.type();
                return type.name() == lhs.type.name()
                       and compatible(type, x.op, d);
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
      return match(std::tie(x.lhs, x.rhs), extract_expr);
    },
    [&](caf::none_t) -> catalog_lookup_result::candidate_info {
      TENZIR_ERROR("{} received an empty expression",
                   detail::pretty_type_name(this));
      TENZIR_ASSERT(false, "invalid expression");
      return all_partitions();
    },
  };
  auto result = match(expr, f);
  result.exp = expr;
  return result;
}

auto catalog_lookup_worker(
  catalog_lookup_worker_actor::stateful_pointer<catalog_lookup_worker_state>
    self,
  tenzir::taxonomies taxonomies, size_t sketch_cache_bytes)
  -> catalog_lookup_worker_actor::behavior_type {
  self->state().engine.taxonomies = std::move(taxonomies);
  self->state().engine.sketches = sketch_cache{sketch_cache_bytes};
  return {
    [self](atom::candidates, expression& expr,
           catalog_snapshot& snapshot) -> caf::result<catalog_lookup_result> {
      if (not snapshot.synopses) {
        return catalog_lookup_result{};
      }
      auto result
        = self->state().engine.lookup(std::move(expr), *snapshot.synopses);
      if (not result) {
        return std::move(result.error());
      }
      return std::move(*result);
    },
    [self](atom::erase, uuid id) -> caf::result<void> {
      // The catalog broadcasts this for a partition that left it or was
      // re-merged under the same id. Ids are otherwise never reused, so a
      // stale cache entry is a memory concern, not a correctness one -- but a
      // re-merge with different content must not be pruned against the old
      // partition's sketches.
      self->state().engine.sketches.erase(id);
      return {};
    },
  };
}

} // namespace tenzir
