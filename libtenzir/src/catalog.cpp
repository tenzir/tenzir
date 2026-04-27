//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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
#include "tenzir/double_synopsis.hpp"
#include "tenzir/duration_synopsis.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/int64_synopsis.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/status.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/uint64_synopsis.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/detail/set_thread_name.hpp>
#include <caf/expected.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <unordered_set>

namespace tenzir {

namespace {

TENZIR_ENUM(catalog_slice_selector, fields, schemas, partitions);

auto collect_synopses(const catalog_state& state)
  -> std::vector<partition_synopsis_pair> {
  auto result = std::vector<partition_synopsis_pair>{};
  result.reserve(state.synopses_per_type.size());
  for (const auto& [schema, id_synopsis_map] : state.synopses_per_type) {
    for (const auto& [id, synopsis] : id_synopsis_map) {
      result.push_back({id, synopsis});
    }
  }
  return result;
}

auto collect_synopses(const catalog_state& state, const expression& filter)
  -> caf::expected<std::vector<partition_synopsis_pair>> {
  auto result = std::vector<partition_synopsis_pair>{};
  const auto candidates = state.lookup(filter);
  if (not candidates) {
    return candidates.error();
  }
  for (const auto& [schema, candidate] : candidates->candidate_infos) {
    const auto& partition_synopses = state.synopses_per_type.find(schema);
    TENZIR_ASSERT(partition_synopses != state.synopses_per_type.end());
    for (const auto& partition : candidate.partition_infos) {
      const auto& synopsis = partition_synopses->second.find(partition.uuid);
      if (synopsis == partition_synopses->second.end()) {
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

auto catalog_state::initialize(std::vector<partition_synopsis_pair> partitions)
  -> caf::result<atom::ok> {
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
  for (auto& [type, flat_data] : flat_data_map) {
    std::ranges::sort(flat_data, std::ranges::less{},
                      &flat_data_list::value_type::first);
    synopses_per_type[type]
      = decltype(synopses_per_type)::value_type::second_type::make_unsafe(
        std::move(flat_data));
  }
  TENZIR_ASSERT(cache);
  cache->unstash();
  cache.reset();
  return atom::ok_v;
}

auto catalog_state::merge(std::vector<partition_synopsis_pair> partitions)
  -> caf::result<atom::ok> {
  for (auto& [id, synopsis] : partitions) {
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
    auto candidates_per_type = lookup_impl(*resolved, type);
    if (candidates_per_type.partition_infos.empty()) {
      continue;
    }
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
  TENZIR_ASSERT(not is<caf::none_t>(expr));
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
    if (not memoized_partitions.partition_infos.empty()
        or partition_synopses.empty()) {
      return memoized_partitions;
    }
    for (const auto& [partition_id, synopsis] : partition_synopses) {
      memoized_partitions.partition_infos.emplace_back(partition_id, *synopsis);
    }
    return memoized_partitions;
  };
  auto f = detail::overload{
    [&](const conjunction& x) -> catalog_lookup_result::candidate_info {
      TENZIR_ASSERT(not x.empty());
      auto i = x.begin();
      auto result = lookup_impl(*i, schema);
      if (not result.partition_infos.empty()) {
        for (++i; i != x.end(); ++i) {
          // TODO: A conjunction means that we can restrict the lookup to the
          // remaining candidates. This could be achived by passing the `result`
          // set to `lookup` along with the child expression.
          auto xs = lookup_impl(*i, schema);
          if (xs.partition_infos.empty()) {
            return xs; // short-circuit
          }
          detail::inplace_intersect(result.partition_infos, xs.partition_infos);
          TENZIR_ASSERT_EXPENSIVE(std::is_sorted(result.partition_infos.begin(),
                                                 result.partition_infos.end()));
        }
      }
      return result;
    },
    [&](const disjunction& x) -> catalog_lookup_result::candidate_info {
      catalog_lookup_result::candidate_info result;
      for (const auto& op : x) {
        // TODO: A disjunction means that we can restrict the lookup to the
        // set of partitions that are outside of the current result set.
        auto xs = lookup_impl(op, schema);
        if (xs.partition_infos.size() == partition_synopses.size()) {
          return xs; // short-circuit
        }
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
        TENZIR_ASSERT(is<data>(x.rhs));
        const auto& rhs = as<data>(x.rhs);
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
              auto cleaned_type = tenzir::match(field.type(), prune);
              // We rely on having a field -> nullptr mapping here for the
              // fields that don't have their own synopsis.
              if (syn) {
                auto opt = syn->lookup(x.op, make_view(rhs));
                if (not opt or *opt) {
                  TENZIR_TRACE("{} selects {} at predicate {}",
                               detail::pretty_type_name(this), part_id, x);
                  result.partition_infos.emplace_back(part_id, *part_syn);
                  break;
                }
                // The field has no dedicated synopsis. Check if there is one
                // for the type in general.
              } else if (auto it = part_syn->type_synopses_.find(cleaned_type);
                         it != part_syn->type_synopses_.end() and it->second) {
                auto opt = it->second->lookup(x.op, make_view(rhs));
                if (not opt or *opt) {
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

auto catalog_state::memusage() const -> size_t {
  size_t result = 0;
  for (const auto& [type, id_synopsis_map] : synopses_per_type) {
    for (const auto& [id, synopsis] : id_synopsis_map) {
      result += synopsis->memusage();
    }
  }
  return result;
}

auto catalog(catalog_actor::stateful_pointer<catalog_state> self)
  -> catalog_actor::behavior_type {
  if (self->getf(caf::local_actor::is_detached_flag)) {
    caf::detail::set_thread_name("tnz.catalog");
  }
  self->state().self = self;
  self->state().taxonomies.concepts = modules::concepts();
  self->state().cache.emplace();
  return {
    [self](atom::start, std::vector<partition_synopsis_pair>& partitions)
      -> caf::result<atom::ok> {
      return self->state().initialize(std::move(partitions));
    },
    [self](atom::merge, std::vector<partition_synopsis_pair>& partitions)
      -> caf::result<atom::ok> {
      if (self->state().cache) {
        return self->state().cache->stash(self, atom::merge_v,
                                          std::move(partitions));
      }
      return self->state().merge(std::move(partitions));
    },
    [self](atom::get) -> caf::result<std::vector<partition_synopsis_pair>> {
      // if (self->state().mail_cache) {
      //   return self->state().stash<std::vector<partition_synopsis_pair>>();
      // }
      return collect_synopses(self->state());
    },
    [self](atom::get, const expression& filter)
      -> caf::result<std::vector<partition_synopsis_pair>> {
      // if (self->state().mail_cache) {
      //   return self->state().stash<std::vector<partition_synopsis_pair>>();
      // }
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
    [self](atom::erase, uuid partition) -> caf::result<atom::ok> {
      if (self->state().cache) {
        return self->state().cache->stash(self, atom::erase_v, partition);
      }
      self->state().erase(partition);
      return atom::ok_v;
    },
    [self](atom::replace, const std::vector<uuid>& old_uuids,
           std::vector<partition_synopsis_pair>& new_synopses)
      -> caf::result<atom::ok> {
      if (self->state().cache) {
        return self->state().cache->stash(self, atom::replace_v, old_uuids,
                                          std::move(new_synopses));
      }
      for (auto const& uuid : old_uuids) {
        self->state().erase(uuid);
      }
      return self->state().merge(std::move(new_synopses));
    },
    [self](atom::candidates, tenzir::query_context query_context)
      -> caf::result<catalog_lookup_result> {
      if (self->state().cache) {
        return self->state().cache->stash(self, atom::candidates_v,
                                          std::move(query_context));
      }
      return self->state().lookup(std::move(query_context.expr));
    },
    [self](atom::get, uuid uuid) -> caf::result<partition_info> {
      if (self->state().cache) {
        return self->state().cache->stash(self, atom::get_v, uuid);
      }
      for (const auto& [type, synopses] : self->state().synopses_per_type) {
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
