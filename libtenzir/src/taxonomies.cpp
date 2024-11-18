//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/taxonomies.hpp"

#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/error.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/type.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include <algorithm>
#include <deque>
#include <stack>
#include <string_view>

namespace tenzir {

concept_ mappend(concept_ lhs, concept_ rhs) {
  if (lhs.description.empty())
    lhs.description = std::move(rhs.description);
  else if (!rhs.description.empty() && lhs.description != rhs.description)
    TENZIR_WARN("encountered conflicting descriptions: \"{}\" and \"{}\"",
                lhs.description, rhs.description);
  for (auto& field : rhs.fields) {
    if (std::count(lhs.fields.begin(), lhs.fields.end(), field) > 0)
      TENZIR_WARN("ignoring duplicate field {}", field);
    else
      lhs.fields.push_back(std::move(field));
  }
  for (auto& c : rhs.concepts) {
    if (std::count(lhs.concepts.begin(), lhs.concepts.end(), c) > 0)
      TENZIR_WARN("ignoring duplicate field {}", c);
    else
      lhs.concepts.push_back(std::move(c));
  }
  return lhs;
}

bool operator==(const concept_& lhs, const concept_& rhs) {
  return lhs.concepts == rhs.concepts && lhs.fields == rhs.fields;
}

const type concepts_data_schema = type{map_type{
  type{string_type{}, {{"key", "concept.name"}}},
  record_type{
    {"concept", concept_::schema()},
  },
}};

bool operator==(const taxonomies& lhs, const taxonomies& rhs) {
  return lhs.concepts == rhs.concepts;
}

std::vector<std::string>
resolve_concepts(const concepts_map& concepts,
                 std::vector<std::string> fields_or_concepts) {
  auto resolved_concepts_cache = std::set<concepts_map::const_iterator>{};
  auto try_resolve_concept
    = [&](auto& self, auto&& field_or_concept,
          const size_t recursion_limit = defaults::max_recursion) -> void {
    if (recursion_limit == 0) {
      TENZIR_WARN("reached recursion limit in concept resolution");
      return;
    }
    if (auto it = concepts.find(field_or_concept); it != concepts.end()) {
      // The field is a concept, so we need to resolve it first, however, we
      // only want to resolve concepts once to avoid an infinite loopö
      if (resolved_concepts_cache.insert(it).second) {
        const auto& [_, concept_] = *it;
        fields_or_concepts.insert(fields_or_concepts.end(),
                                  concept_.fields.begin(),
                                  concept_.fields.end());
        for (const auto& nested_concept : concept_.concepts)
          self(self, nested_concept, recursion_limit - 1);
      }
    } else {
      // The field is not a concept, so we just add it back to the vector.
      fields_or_concepts.push_back(
        std::forward<decltype(field_or_concept)>(field_or_concept));
    }
  };
  for (auto&& field_or_concept : std::exchange(fields_or_concepts, {}))
    try_resolve_concept(try_resolve_concept, std::move(field_or_concept));
  return fields_or_concepts;
}

static bool contains(const type& schema, const std::string& x,
                     relational_operator op, const tenzir::data& data) {
  const auto* rt = try_as<record_type>(&schema);
  TENZIR_ASSERT(rt);
  for (const auto& offset : rt->resolve_key_suffix(x, schema.name())) {
    if (compatible(rt->field(offset).type, op, data))
      return true;
  }
  return false;
}

caf::expected<expression>
resolve(const taxonomies& ts, const expression& e, const type& schema) {
  return for_each_predicate(
    e, [&](const auto& pred) -> caf::expected<expression> {
      // TODO: Rename appropriately.
      auto resolve_concepts = [&](const std::string& field_name,
                                  relational_operator op,
                                  const tenzir::data& data,
                                  auto make_predicate) {
        // This algorithm recursivly looks up items form the concepts map and
        // generates a predicate for every discovered name that is not a concept
        // itself.
        auto c = ts.concepts.find(field_name);
        if (c == ts.concepts.end())
          return expression{std::move(pred)};
        // The log of all referenced concepts that we tried to resolve already.
        // This is a deque instead of a stable_set because we don't want
        // push_back to invalidate the `current` iterator.
        std::deque<std::string> log;
        // All fields that the concept resolves to either directly or indirectly
        // through referenced concepts.
        detail::stable_set<std::string> target_fields;
        auto load_definition = [&](const concept_& def) {
          // Create the union of all fields by inserting into the set.
          target_fields.insert(def.fields.begin(), def.fields.end());
          // Insert only those concepts into the queue that aren't in there yet,
          // this prevents infinite loops through circular references between
          // concepts.
          for (auto& x : def.concepts)
            if (std::find(log.begin(), log.end(), x) == log.end())
              log.push_back(x);
        };
        load_definition(c->second);
        // We iterate through the log while appending referenced concepts in
        // load_definition.
        for (auto current : log)
          if (auto ref = ts.concepts.find(current); ref != ts.concepts.end())
            load_definition(ref->second);
        // Transform the target_fields into new predicates.
        disjunction d;
        auto make_pred = make_predicate(op, data);
        for (auto& x : target_fields) {
          if (!schema || contains(schema, x, op, data))
            d.emplace_back(make_pred(std::move(x)));
        }
        switch (d.size()) {
          case 0:
            return expression{std::move(pred)};
          case 1:
            return d[0];
          default:
            return expression{d};
        }
      };
      if (auto data = try_as<tenzir::data>(&pred.rhs)) {
        if (auto fe = try_as<field_extractor>(&pred.lhs)) {
          return resolve_concepts(
            fe->field, pred.op, *data,
            [&](relational_operator op, const tenzir::data& o) {
              return [&, op](const std::string& item) {
                return predicate{field_extractor{item}, op, o};
              };
            });
        }
      }
      if (auto data = try_as<tenzir::data>(&pred.lhs)) {
        if (auto fe = try_as<field_extractor>(&pred.rhs)) {
          return resolve_concepts(
            fe->field, pred.op, *data,
            [&](relational_operator op, const tenzir::data& o) {
              return [&, op](const std::string& item) {
                return predicate{o, op, field_extractor{item}};
              };
            });
        }
      }
      return expression{pred};
    });
}

} // namespace tenzir
