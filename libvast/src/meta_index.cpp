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

#include "vast/meta_index.hpp"

#include "vast/data.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/set_operations.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/system/atoms.hpp"
#include "vast/table_slice.hpp"
#include "vast/time.hpp"

namespace vast {

void meta_index::add(const uuid& partition, const table_slice& slice) {
  auto& part_synopsis = partition_synopses_[partition];
  auto& layout = slice.layout();
  if (blacklisted_layouts_.count(layout) == 1)
    return;
  auto i = part_synopsis.find(layout);
  table_synopsis* table_syn;
  if (i != part_synopsis.end()) {
    table_syn = &i->second;
  } else {
    // Create new synopses for a layout we haven't seen before.
    i = part_synopsis.emplace(layout, table_synopsis{}).first;
    table_syn = &i->second;
    for (auto& field : layout.fields) {
      auto syn = has_skip_attribute(field.type)
                   ? nullptr
                   : factory<synopsis>::make(field.type, synopsis_options_);
      if (table_syn->emplace_back(std::move(syn)) != nullptr)
        VAST_DEBUG(this, "created new synopsis structure for type", field.type);
    }
    // If we couldn't create a single synopsis for the layout, we will no
    // longer attempt to create synopses in the future.
    auto is_nullptr = [](auto& x) { return x == nullptr; };
    if (std::all_of(table_syn->begin(), table_syn->end(), is_nullptr)) {
      VAST_DEBUG(this, "could not create a synopsis for layout:", layout);
      blacklisted_layouts_.insert(layout);
    }
  }
  VAST_ASSERT(table_syn->size() == slice.columns());
  for (size_t col = 0; col < slice.columns(); ++col)
    if (auto& syn = (*table_syn)[col])
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col);
        if (!caf::holds_alternative<caf::none_t>(view))
          syn->add(std::move(view));
      }
}

std::vector<uuid> meta_index::lookup(const expression& expr) const {
  VAST_ASSERT(!caf::holds_alternative<caf::none_t>(expr));
  // TODO: we could consider a flat_set<uuid> here, which would then have
  // overloads for inplace intersection/union and simplify the implementation
  // of this function a bit.
  using result_type = std::vector<uuid>;
  auto all_partitions = [&] {
    result_type result;
    result.reserve(partition_synopses_.size());
    std::transform(partition_synopses_.begin(),
                   partition_synopses_.end(),
                   std::back_inserter(result),
                   [](auto& x) { return x.first; });
    std::sort(result.begin(), result.end());
    return result;
  };
  return caf::visit(detail::overload(
    [&](const conjunction& x) -> result_type {
      VAST_ASSERT(!x.empty());
      auto i = x.begin();
      auto result = lookup(*i);
      if (!result.empty())
        for (++i; i != x.end(); ++i) {
          auto xs = lookup(*i);
          if (xs.empty())
            return xs; // short-circuit
          detail::inplace_intersect(result, xs);
        }
      return result;
    },
    [&](const disjunction& x) -> result_type {
      result_type result;
      for (auto& op : x) {
        auto xs = lookup(op);
        if (xs.size() == partition_synopses_.size())
          return xs; // short-circuit
        detail::inplace_unify(result, xs);
      }
      return result;
    },
    [&](const negation&) -> result_type {
      // We cannot handle negations, because a synopsis may return false
      // positives, and negating such a result may cause false negatives.
      return all_partitions();
    },
    [&](const predicate& x) -> result_type {
      // Performs a lookup on all *matching* synopses with operator and data
      // from the predicate of the expression. The match function uses a record
      // field to determine whether the synopsis should be queried.
      auto search = [&](auto match) {
        VAST_ASSERT(caf::holds_alternative<data>(x.rhs));
        auto& rhs = caf::get<data>(x.rhs);
        result_type result;
        auto found_matching_synopsis = false;
        // We factor the nested loop into a lambda so that we can abort the
        // iteration more easily with a return statement.
        auto lookup = [&](auto& part_id, auto& part_syn) {
          for (auto& [layout, table_syn] : part_syn)
            for (size_t i = 0; i < table_syn.size(); ++i)
              if (table_syn[i] && match(layout.fields[i])) {
                found_matching_synopsis = true;
                auto opt = table_syn[i]->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  result.push_back(part_id);
                  return;
                }
              }
        };
        for (auto& [part_id, part_syn] : partition_synopses_)
          lookup(part_id, part_syn);
        std::sort(result.begin(), result.end());
        return found_matching_synopsis ? result : all_partitions();
      };
      return caf::visit(detail::overload(
        [&](const attribute_extractor& lhs, const data& d) -> result_type {
          if (lhs.attr == system::time_atom::value) {
            auto pred = [](auto& field) {
              return has_attribute(field.type, "time");
            };
            return search(pred);
          } else if (lhs.attr == system::type_atom::value) {
            result_type result;
            for (auto& [part_id, part_syn] : partition_synopses_)
              for (auto& pair : part_syn) {
                VAST_INFO(this, "checking", pair.first.name());
                if (evaluate(pair.first.name(), x.op, d)) {
                  result.push_back(part_id);
                  break;
                }
              }
            return result;
          }
          VAST_WARNING(this, "cannot process attribute extractor:", lhs.attr);
          return all_partitions();
        },
        [&](const key_extractor& lhs, const data&) -> result_type {
          auto pred = [&](auto& field) {
            return detail::ends_with(field.name, lhs.key);
          };
          return search(pred);
        },
        [&](const type_extractor& lhs, const data&) -> result_type {
          auto pred = [&](auto& field) { return field.type == lhs.type; };
          return search(pred);
        },
        [&](const auto&, const auto&) -> result_type {
          VAST_WARNING(this, "cannot process predicate:", x);
          return all_partitions();
        }
      ), x.lhs, x.rhs);
    },
    [&](caf::none_t) -> result_type {
      VAST_ERROR(this, "received an empty expression");
      VAST_ASSERT(!"invalid expression");
      return all_partitions();
    }
  ), expr);
}

synopsis_options& meta_index::factory_options() {
  return synopsis_options_;
}

caf::error inspect(caf::serializer& sink, const meta_index& x) {
  return sink(x.synopsis_options_, x.partition_synopses_);
}

caf::error inspect(caf::deserializer& source, meta_index& x) {
  return source(x.synopsis_options_, x.partition_synopses_);
}

} // namespace vast
