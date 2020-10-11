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
#include <deque>
#include <stack>

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
      // The log of all fields that we tried to resolve to concepts already.
      // This is a deque instead of a stable_set because we don't want
      // push_back to invalidate the `current` iterator.
      std::deque<std::string> log;
      log.push_back(fe->field);
      // The log is partitioned into 3 segments:
      //  1. The item we're presently looking for (current)
      //  2. The items that have been looked for already. Those are not
      //     discarded because we must not enqueue any items more than once
      //  3. The items that still need to be looked for
      for (auto current = log.begin(); current != log.end(); ++current) {
        auto& x = *current;
        auto i = concepts.find(x);
        if (i != concepts.end()) {
          // x is a concpept, push target items to the back of the log, we
          // will check if they are concepts themselves later.
          for (auto ri = i->second.begin(); ri != i->second.end(); ++ri) {
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
