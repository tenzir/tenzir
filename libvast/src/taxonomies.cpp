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
#include "vast/error.hpp"
#include "vast/expression.hpp"

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
      return make_error(ec::convert_error, "name is not a string:", *n);
    auto fs = c->find("fields");
    if (fs == c->end())
      return make_error(ec::convert_error, "concept has no fields:", d);
    if (const auto& fields = caf::get_if<list>(&fs->second)) {
      for (auto& f : *fields) {
        auto field = caf::get_if<std::string>(&f);
        if (!field)
          return make_error(ec::convert_error, "field is not a string:", f);
        out[*name].push_back(*field);
      }
    } else {
      return make_error(ec::convert_error, "fields is not a list:", d);
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
      disjunction d;
      // The log of all fields that we tried to resolve to concepts already.
      // This is a deque instead of a stable_set because we don't want
      // push_back to invalidate the `current` iterator.
      std::deque<std::string> log;
      log.push_back(field_name);
      // The log is partitioned into 3 segments:
      //  1. The item we're presently looking for (current)
      //  2. The items that have been looked for already. Those are not
      //     discarded because we must not enqueue any items more than once
      //  3. The items that still need to be looked for
      for (auto current = log.begin(); current != log.end(); ++current) {
        auto& x = *current;
        auto concept_ = concepts.find(x);
        if (concept_ != concepts.end()) {
          // x is a concpept, push target items to the back of the log, we
          // will check if they are concepts themselves later.
          auto& replacements = concept_->second;
          // ri abbreviates "replacement iterator".
          for (auto ri = replacements.begin(); ri != replacements.end(); ++ri) {
            // We need to prevent duplicate additions to the queue for 2
            // reasons:
            //  1. We don't want to add the same predicate to the expression
            //     twice
            //  2. If the target is itself a concept and it was already looked
            //     for, adding it again would create an infinite loop.
            if (std::find(log.begin(), log.end(), *ri) == log.end())
              log.push_back(*ri);
          }
        } else {
          // x is not a concept, that means it is a field and we create a
          // predicate for it.
          if (!prune || contains(seen, x, op, data))
            d.emplace_back(make_predicate(x));
        }
      }
      switch (d.size()) {
        case 0:
          return expression{};
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
