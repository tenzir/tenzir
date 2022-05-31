//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/taxonomies.hpp"

#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"
#include "vast/type_set.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include <algorithm>
#include <deque>
#include <stack>
#include <string_view>

namespace vast {

concept_ mappend(concept_ lhs, concept_ rhs) {
  if (lhs.description.empty())
    lhs.description = std::move(rhs.description);
  else if (!rhs.description.empty() && lhs.description != rhs.description)
    VAST_WARN("encountered conflicting descriptions: \"{}\" and \"{}\"",
              lhs.description, rhs.description);
  for (auto& field : rhs.fields) {
    if (std::count(lhs.fields.begin(), lhs.fields.end(), field) > 0)
      VAST_WARN("ignoring duplicate field {}", field);
    else
      lhs.fields.push_back(std::move(field));
  }
  for (auto& c : rhs.concepts) {
    if (std::count(lhs.concepts.begin(), lhs.concepts.end(), c) > 0)
      VAST_WARN("ignoring duplicate field {}", c);
    else
      lhs.concepts.push_back(std::move(c));
  }
  return lhs;
}

bool operator==(const concept_& lhs, const concept_& rhs) {
  return lhs.concepts == rhs.concepts && lhs.fields == rhs.fields;
}

const record_type& concept_::layout() noexcept {
  static const auto result = record_type{
    {"description", string_type{}},
    {"fields", list_type{string_type{}}},
    {"concepts", list_type{string_type{}}},
  };
  return result;
}

const type concepts_data_layout = type{map_type{
  type{string_type{}, {{"key", "concept.name"}}},
  record_type{
    {"concept", concept_::layout()},
  },
}};

bool operator==(const model& lhs, const model& rhs) {
  return lhs.definition == rhs.definition;
}

const record_type& model::layout() noexcept {
  static const auto result = record_type{
    {"description", string_type{}},
    {"definition", list_type{string_type{}}},
  };
  return result;
}

const type models_data_layout = type{map_type{
  type{string_type{}, {{"key", "model.name"}}},
  record_type{
    {"model", model::layout()},
  },
}};

bool operator==(const taxonomies& lhs, const taxonomies& rhs) {
  return lhs.concepts == rhs.concepts && lhs.models == rhs.models;
}

std::vector<std::string>
resolve_concepts(const concepts_map& concepts,
                 std::vector<std::string> fields_or_concepts) {
  auto resolved_concepts_cache = std::set<concepts_map::const_iterator>{};
  auto try_resolve_concept
    = [&](auto& self, auto&& field_or_concept,
          const size_t recursion_limit = defaults::max_recursion) -> void {
    if (recursion_limit == 0) {
      VAST_WARN("reached recursion limit in concept resolution");
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

static bool
contains(const std::map<std::string, type_set>& seen, const std::string& x,
         relational_operator op, const vast::data& data) {
  std::string::size_type pos = 0;
  while ((pos = x.find('.', pos)) != std::string::npos) {
    auto i = seen.find(std::string{std::string_view{x.c_str(), pos}});
    if (i != seen.end()) {
      // A prefix of x matches an existing layout.
      auto field = x.substr(pos + 1);
      return std::any_of(
        i->second.begin(), i->second.end(), [&](const type& t) {
          if (const auto& layout = caf::get_if<record_type>(&t)) {
            if (auto offset = layout->resolve_key(field))
              return compatible(layout->field(*offset).type, op, data);
          }
          return false;
        });
    }
    ++pos;
  }
  return false;
}

static caf::expected<expression>
resolve_impl(const taxonomies& ts, const expression& e,
             const std::map<std::string, type_set>& seen, bool prune) {
  return for_each_predicate(
    e, [&](const auto& pred) -> caf::expected<expression> {
      // TODO: Rename appropriately.
      auto resolve_concepts = [&](const std::string& field_name,
                                  relational_operator op,
                                  const vast::data& data, auto make_predicate) {
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
          if (!prune || contains(seen, x, op, data))
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
      auto resolve_models
        = [&](const std::string& field_name, relational_operator op,
              const vast::data& data,
              auto make_predicate) -> caf::expected<expression> {
        auto r = caf::get_if<record>(&data);
        if (!r)
          // Models can only be compared to records, so if the data side is
          // not a record, we move to the concept substitution phase directly.
          return resolve_concepts(field_name, op, data, make_predicate);
        if (r->empty())
          return expression{caf::none};
        auto it = ts.models.find(field_name);
        if (it == ts.models.end())
          return resolve_concepts(field_name, op, data, make_predicate);
        // We have a model predicate.
        // ==========================
        // The model definition forms a tree that contains models as non-leaf
        // nodes and concepts as leafs. For model substition we need to iterate
        // over the leafs in the order of definition, which is left to right.
        // The levels stack is used to keep track of the current position at
        // each level of the tree.
        auto level_1 = std::pair{it->second.definition.begin(),
                                 it->second.definition.end()};
        auto levels = std::stack{std::vector{std::move(level_1)}};
        auto descend = [&] {
          for (auto child_component = ts.models.find(*levels.top().first);
               child_component != ts.models.end();
               child_component = ts.models.find(*levels.top().first)) {
            auto& child_def = child_component->second.definition;
            levels.emplace(child_def.begin(), child_def.end());
          }
        };
        // Move the cursor to the leftmost leaf in the tree.
        descend();
        auto next_leaf = [&] {
          // Update the levels stack; explicit scope for clarity.
          while (!levels.empty() && ++levels.top().first == levels.top().second)
            levels.pop();
          if (!levels.empty()) {
            descend();
            // Empty models ought to be rejected at load time.
            VAST_ASSERT(levels.top().first != levels.top().second);
          }
        };
        // The conjunction for all model concepts that are restriced by a value
        // in rec.
        conjunction restricted;
        // The conjunction for all model concepts that aren't specified in rec.
        conjunction unrestricted;
        auto abs_op = is_negated(op) ? negate(op) : op;
        auto insert_meta_field_predicate = [&] {
          auto make_meta_field_predicate = [&](relational_operator op,
                                               const vast::data&) {
            return [&, op](std::string item) {
              return predicate{selector{selector::field}, op, vast::data{item}};
            };
          };
          unrestricted.emplace_back(
            resolve_concepts(*levels.top().first, relational_operator::equal,
                             caf::none, make_meta_field_predicate));
        };
        auto named = !r->begin()->first.empty();
        if (named) {
          // TODO: Nested records of the form
          // <src_endpoint: <1.2.3.4, _>, process_filename: "svchost.exe">
          // are currently not supported.
          for (; !levels.empty(); next_leaf()) {
            // TODO: Use `ends_with` for better ergonomics.
            // TODO: Remove matched entries and check mismatched concepts.
            auto concept_field = r->find(*levels.top().first);
            if (concept_field == r->end())
              insert_meta_field_predicate();
            else
              restricted.emplace_back(
                resolve_concepts(*levels.top().first, abs_op,
                                 concept_field->second, make_predicate));
          }
        } else {
          auto value_iterator = r->begin();
          for (; !levels.empty(); next_leaf(), ++value_iterator) {
            if (value_iterator == r->end())
              // The provided record is shorter than the matched concept.
              // TODO: This error could be rendered in a way that makes it
              // clear how the mismatch happened. For example:
              //   src_ip, src_port,  dst_ip, dst_port, proto
              // <      _,        _, 1.2.3.4,        _>
              //                                        ^~~~~
              //                                        not enough fields provided
              return caf::make_error(ec::invalid_query, *r,
                                     "doesn't match the model:", it->first);
            if (caf::holds_alternative<caf::none_t>(value_iterator->second))
              insert_meta_field_predicate();
            else
              restricted.emplace_back(
                resolve_concepts(*levels.top().first, abs_op,
                                 value_iterator->second, make_predicate));
          }
          if (value_iterator != r->end()) {
            // The provided record is longer than the matched concept.
            // TODO: This error could be rendered in a way that makes it
            // clear how the mismatch happened. For example:
            //   src_ip, src_port,  dst_ip, dst_port, proto
            // <      _,        _, 1.2.3.4,        _,     _, "tcp">
            //                                               ^~~~~
            //                                               too many fields
            //                                               provided
            return caf::make_error(ec::invalid_query, *r,
                                   "doesn't match the model:", it->first);
          }
        }
        expression expr;
        switch (restricted.size()) {
          case 0: {
            return unrestricted;
          }
          case 1: {
            expr = restricted[0];
            break;
          }
          default: {
            expr = expression{std::move(restricted)};
            break;
          }
        }
        if (is_negated(op))
          expr = negation{std::move(expr)};
        if (unrestricted.empty())
          return expr;
        unrestricted.push_back(expr);
        return unrestricted;
      };
      if (auto data = caf::get_if<vast::data>(&pred.rhs)) {
        if (auto fe = caf::get_if<extractor>(&pred.lhs)) {
          return resolve_models(fe->value, pred.op, *data,
                                [&](relational_operator op,
                                    const vast::data& o) {
                                  return [&, op](const std::string& item) {
                                    return predicate{extractor{item}, op, o};
                                  };
                                });
        }
      }
      if (auto data = caf::get_if<vast::data>(&pred.lhs)) {
        if (auto fe = caf::get_if<extractor>(&pred.rhs)) {
          return resolve_models(fe->value, pred.op, *data,
                                [&](relational_operator op,
                                    const vast::data& o) {
                                  return [&, op](const std::string& item) {
                                    return predicate{o, op, extractor{item}};
                                  };
                                });
        }
      }
      return expression{pred};
    });
}

caf::expected<expression> resolve(const taxonomies& ts, const expression& e) {
  return resolve_impl(ts, e, {}, false);
}

caf::expected<expression> resolve(const taxonomies& ts, const expression& e,
                                  const std::map<std::string, type_set>& seen) {
  return resolve_impl(ts, e, seen, true);
}

} // namespace vast
