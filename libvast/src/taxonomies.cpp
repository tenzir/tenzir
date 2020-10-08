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

#include "vast/error.hpp"
#include "vast/expression.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include <algorithm>
#include <stack>
#include <vector>

namespace vast {

bool operator==(const taxonomies& lhs, const taxonomies& rhs) {
  return lhs.concepts == rhs.concepts && lhs.models == rhs.models;
}

caf::error inspect(caf::serializer& sink, const taxonomies_ptr& x) {
  return sink(*x);
}

caf::error inspect(caf::deserializer& source, taxonomies_ptr& x) {
  if (x.use_count() > 1)
    return make_error(ec::logic_error, "deserializing into object with multiple"
                                       "owners violates precondition");
  return source(*x);
}

static expression
resolve_concepts(const concepts_t& concepts, const expression& orig) {
  return for_each_predicate(orig, [&](const auto& pred) {
    if (auto fe = caf::get_if<field_extractor>(&pred.lhs)) {
      // This algorithm recursivly looks up items form the concepts map and
      // generates a predicate for every discovered name that is not a concept
      // itself.
      disjunction d;
      std::stack<std::string> s;
      std::vector<std::string> tried;
      s.push(fe->field);
      while (!s.empty()) {
        auto x = s.top();
        s.pop();
        auto i = concepts.find(x);
        tried.push_back(x);
        if (i != concepts.end()) {
          for (auto ri = i->second.rbegin(); ri != i->second.rend(); ++ri) {
            // Cycle breaker.
            if (std::find(tried.begin(), tried.end(), *ri) == tried.end())
              s.push(*ri);
          }
        } else {
          d.emplace_back(predicate{field_extractor{x}, pred.op, pred.rhs});
        }
      }
      return expression{d};
    }
    return expression{pred};
  });
}

expression resolve(const taxonomies& t, const expression& orig) {
  return resolve_concepts(t.concepts, orig);
}

} // namespace vast
