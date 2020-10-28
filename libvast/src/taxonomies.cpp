/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/taxonomies.hpp"

#include "vast/concept/printable/vast/data.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include <algorithm>
#include <deque>
#include <string_view>

namespace vast {

caf::error convert(const data& d, concepts_type& out) {
  if (const auto& c = caf::get_if<record>(&d)) {
    auto n = c->find("name");
    if (n == c->end())
      return make_error(ec::convert_error, "concept has no name:", d);
    auto name = caf::get_if<std::string>(&n->second);
    if (!name)
      return make_error(ec::convert_error, "concept name is not a string:", *n);
    auto& dest = out.data[*name];
    auto fs = c->find("fields");
    if (fs != c->end()) {
      if (const auto& fields = caf::get_if<list>(&fs->second)) {
        for (auto& f : *fields) {
          auto field = caf::get_if<std::string>(&f);
          if (!field)
            return make_error(ec::convert_error, "field in", *name,
                              "is not a string:", f);
          dest.fields.push_back(*field);
        }
      } else {
        return make_error(ec::convert_error, "fields in", *name,
                          "is not a list:", fs->second);
      }
    }
    auto cs = c->find("concepts");
    if (cs != c->end()) {
      if (const auto& concepts = caf::get_if<list>(&cs->second)) {
        for (auto& c : *concepts) {
          auto concept_ = caf::get_if<std::string>(&c);
          if (!concept_)
            return make_error(ec::convert_error, "concept in", *name,
                              "is not a string:", c);
          dest.concepts.push_back(*concept_);
        }
      } else {
        return make_error(ec::convert_error, "concepts in", *name,
                          "is not a list:", cs->second);
      }
    }
    auto desc = c->find("description");
    if (desc != c->end()) {
      if (auto description = caf::get_if<std::string>(&desc->second)) {
        if (dest.description.empty())
          dest.description = *description;
        else if (dest.description != *description)
          VAST_WARNING_ANON("encountered conflicting descriptions for",
                            *name + ": \"" + dest.description + "\" and \""
                              + *description + "\"");
      }
    }
  } else {
    return make_error(ec::convert_error, "concept is not a record:", d);
  }
  return caf::none;
}

caf::error extract_concepts(const data& d, concepts_type& out) {
  if (const auto& xs = caf::get_if<list>(&d)) {
    for (const auto& item : *xs) {
      if (const auto& x = caf::get_if<record>(&item)) {
        auto n = x->find("concept");
        if (n == x->end())
          continue;
        if (auto err = convert(n->second, out))
          return err;
      }
    }
  }
  return caf::none;
}

caf::expected<concepts_type> extract_concepts(const data& d) {
  concepts_type result;
  if (auto err = extract_concepts(d, result))
    return err;
  return result;
}

bool operator==(const concepts_type::definition& lhs,
                const concepts_type::definition& rhs) {
  return lhs.concepts == rhs.concepts && lhs.fields == rhs.fields;
}

bool operator==(const concepts_type& lhs, const concepts_type& rhs) {
  return lhs.data == rhs.data;
}

bool operator==(const taxonomies& lhs, const taxonomies& rhs) {
  return lhs.concepts == rhs.concepts && lhs.models == rhs.models;
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
      return std::any_of(i->second.value.begin(), i->second.value.end(),
                         [&](const type& t) {
                           if (auto r = caf::get_if<record_type>(&t)) {
                             if (auto f = r->find(field))
                               return compatible(f->type, op, data);
                           }
                           return false;
                         });
    }
    ++pos;
  }
  return false;
}

static expression
resolve_concepts(const concepts_type& concepts, const expression& e,
                 const std::map<std::string, type_set>& seen, bool prune) {
  return for_each_predicate(e, [&](const auto& pred) {
    auto run = [&](const std::string& field_name, relational_operator op,
                   const vast::data& data, auto make_predicate) {
      // This algorithm recursivly looks up items form the concepts map and
      // generates a predicate for every discovered name that is not a concept
      // itself.
      auto concept_ = concepts.data.find(field_name);
      if (concept_ == concepts.data.end())
        return expression{std::move(pred)};
      // The log of all referenced concepts that we tried to resolve already.
      // This is a deque instead of a stable_set because we don't want
      // push_back to invalidate the `current` iterator.
      std::deque<std::string> log;
      // All fields that the concept is resolve to either directly or indirectly
      // through referenced concepts.
      detail::stable_set<std::string> target_fields;
      auto handle_def = [&](const concepts_type::definition& def) {
        // Create the union of all fields by inserting into the set.
        target_fields.insert(def.fields.begin(), def.fields.end());
        // Insert only those concepts into the queue that aren't in there yet,
        // this prevents infinite loops through circular references between
        // concepts.
        for (auto& x : def.concepts) {
          if (std::find(log.begin(), log.end(), x) == log.end())
            log.push_back(x);
        }
      };
      handle_def(concept_->second);
      // We iterate through the log while appending referenced concepts in
      // handle_def.
      for (auto current : log) {
        auto ref_concept = concepts.data.find(current);
        if (ref_concept != concepts.data.end())
          handle_def(ref_concept->second);
      }
      // Transform the target_fields into new predicates.
      disjunction d;
      for (auto& x : target_fields) {
        if (!prune || contains(seen, x, op, data))
          d.emplace_back(make_predicate(std::move(x)));
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
    if (auto fe = caf::get_if<field_extractor>(&pred.lhs)) {
      if (auto data = caf::get_if<vast::data>(&pred.rhs)) {
        return run(fe->field, pred.op, *data, [&](const std::string& item) {
          return predicate{field_extractor{item}, pred.op, pred.rhs};
        });
      }
    }
    if (auto fe = caf::get_if<field_extractor>(&pred.rhs)) {
      if (auto data = caf::get_if<vast::data>(&pred.lhs)) {
        return run(fe->field, flip(pred.op), *data,
                   [&](const std::string& item) {
                     return predicate{pred.lhs, pred.op, field_extractor{item}};
                   });
      }
    }
    return expression{pred};
  });
}

expression resolve(const taxonomies& t, const expression& e) {
  return resolve_concepts(t.concepts, e, {}, false);
}

expression resolve(const taxonomies& t, const expression& e,
                   const std::map<std::string, type_set>& seen) {
  return resolve_concepts(t.concepts, e, seen, true);
}

} // namespace vast
