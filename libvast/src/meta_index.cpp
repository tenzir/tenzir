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

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

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
  // of this function a bit. This would also simplify the maintainance of a
  // critical invariant: partition UUIDs must be sorted. Otherwise the
  // invariants of the inplace union and intersection algorithms are violated,
  // leading to wrong results. This invariant is easily violated because we
  // currently just append results to the candidate vector, so all places where
  // we return an assembled set must ensure the post-condition of returning a
  // sorted list.
  using result_type = std::vector<uuid>;
  result_type memoized_partitions;
  auto all_partitions = [&] {
    if (!memoized_partitions.empty() || partition_synopses_.empty())
      return memoized_partitions;
    memoized_partitions.reserve(partition_synopses_.size());
    std::transform(partition_synopses_.begin(), partition_synopses_.end(),
                   std::back_inserter(memoized_partitions),
                   [](auto& x) { return x.first; });
    std::sort(memoized_partitions.begin(), memoized_partitions.end());
    return memoized_partitions;
  };
  auto f = detail::overload(
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
          VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
        }
      return result;
    },
    [&](const disjunction& x) -> result_type {
      result_type result;
      for (auto& op : x) {
        auto xs = lookup(op);
        VAST_ASSERT(std::is_sorted(xs.begin(), xs.end()));
        if (xs.size() == partition_synopses_.size())
          return xs; // short-circuit
        detail::inplace_unify(result, xs);
        VAST_ASSERT(std::is_sorted(result.begin(), result.end()));
      }
      return result;
    },
    [&](const negation&) -> result_type {
      // We cannot handle negations, because a synopsis may return false
      // positives, and negating such a result may cause false
      // negatives.
      return all_partitions();
    },
    [&](const predicate& x) -> result_type {
      // Performs a lookup on all *matching* synopses with operator and
      // data from the predicate of the expression. The match function
      // uses a record field to determine whether the synopsis should be
      // queried.
      auto search = [&](auto match) {
        VAST_ASSERT(caf::holds_alternative<data>(x.rhs));
        auto& rhs = caf::get<data>(x.rhs);
        result_type result;
        auto found_matching_synopsis = false;
        // We factor the nested loop into a lambda so that we can abort
        // the iteration more easily with a return statement.
        auto lookup = [&](auto& part_id, auto& part_syn) {
          VAST_DEBUG(this, "checks", part_id, "at predicate", x);
          for (auto& [layout, table_syn] : part_syn)
            for (size_t i = 0; i < table_syn.size(); ++i)
              if (table_syn[i] && match(layout.fields[i])) {
                found_matching_synopsis = true;
                auto opt = table_syn[i]->lookup(x.op, make_view(rhs));
                if (!opt || *opt) {
                  VAST_DEBUG(this, "selects", part_id, "at predicate", x);
                  result.push_back(part_id);
                  return;
                }
              }
        };
        for (auto& [part_id, part_syn] : partition_synopses_)
          lookup(part_id, part_syn);
        // Re-establish potentially violated invariant.
        std::sort(result.begin(), result.end());
        return found_matching_synopsis ? result : all_partitions();
      };
      auto extract_expr = detail::overload(
        [&](const attribute_extractor& lhs, const data& d) -> result_type {
          if (lhs.attr == system::timestamp_atom::value) {
            auto pred = [](auto& field) {
              return has_attribute(field.type, "timestamp");
            };
            return search(pred);
          } else if (lhs.attr == system::type_atom::value) {
            result_type result;
            for (auto& [part_id, part_syn] : partition_synopses_)
              for (auto& pair : part_syn)
                if (evaluate(pair.first.name(), x.op, d)) {
                  result.push_back(part_id);
                  break;
                }
            // Re-establish potentially violated invariant.
            std::sort(result.begin(), result.end());
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
        });
      return caf::visit(extract_expr, x.lhs, x.rhs);
    },
    [&](caf::none_t) -> result_type {
      VAST_ERROR(this, "received an empty expression");
      VAST_ASSERT(!"invalid expression");
      return all_partitions();
    });
  return caf::visit(f, expr);
}

caf::settings& meta_index::factory_options() {
  return synopsis_options_;
}

caf::error inspect(caf::serializer& sink, const meta_index& x) {
  return sink(x.synopsis_options_, x.partition_synopses_,
              x.blacklisted_layouts_);
}

caf::error inspect(caf::deserializer& source, meta_index& x) {
  return source(x.synopsis_options_, x.partition_synopses_,
                x.blacklisted_layouts_);
}

// Perform a deep equality comparison for meta indices. This is slow and we only
// need it to test serializer / deserializer roundtrips.
bool deep_equals(const meta_index& lhs, const meta_index& rhs) {
  // Fail early when the trivial comparisons are false.
  if (lhs.synopsis_options_ != rhs.synopsis_options_
      || lhs.blacklisted_layouts_ != rhs.blacklisted_layouts_)
    return false;
  auto to_map = [](const auto& unordered) {
    // TODO: Use the deduction guide of std::map directly once we update to a
    // newer version of libc++ for testing.
    using unordered_map = std::decay_t<decltype(unordered)>;
    using map = std::map<typename unordered_map::key_type,
                         typename unordered_map::mapped_type>;
    return map{unordered.begin(), unordered.end()};
  };
  auto deep_equals = [](const auto& lhs, const auto& rhs) {
    if (lhs == rhs)
      return true;
    if (!lhs || !rhs)
      return false;
    return *lhs == *rhs;
  };
  // Perform a deep comparison of the underlying synopsis pointers.
  auto lhs_ps_sorted = to_map(lhs.partition_synopses_);
  auto rhs_ps_sorted = to_map(rhs.partition_synopses_);
  return std::equal(
    lhs_ps_sorted.begin(), lhs_ps_sorted.end(), rhs_ps_sorted.begin(),
    rhs_ps_sorted.end(), [&](const auto& lhs, const auto& rhs) {
      // first is uuid, second is partition_synopsis
      auto lhs_ts_sorted = to_map(lhs.second);
      auto rhs_ts_sorted = to_map(rhs.second);
      return lhs.first == rhs.first
             && std::equal(lhs_ts_sorted.begin(), lhs_ts_sorted.end(),
                           rhs_ts_sorted.begin(), rhs_ts_sorted.end(),
                           [&](const auto& lhs, const auto& rhs) {
                             // first is record_type, second is table_synopsis
                             return lhs.first == rhs.first
                                    && std::equal(lhs.second.begin(),
                                                  lhs.second.end(),
                                                  rhs.second.begin(),
                                                  rhs.second.end(),
                                                  deep_equals);
                           });
    });
}

caf::expected<flatbuffers::Offset<fbs::MetaIndex>>
pack(flatbuffers::FlatBufferBuilder& builder, const meta_index& x) {
  std::vector<char> buffer;
  caf::binary_serializer sink{nullptr, buffer};
  if (auto error = sink(x))
    return error;
  auto data_ptr = reinterpret_cast<const uint8_t*>(buffer.data());
  auto data = builder.CreateVector(data_ptr, buffer.size());
  fbs::MetaIndexBuilder meta_index_builder{builder};
  meta_index_builder.add_state(data);
  return meta_index_builder.Finish();
}

caf::error unpack(const fbs::MetaIndex& x, meta_index& y) {
  auto ptr = reinterpret_cast<const char*>(x.state()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.state()->size()};
  return source(y);
}

} // namespace vast
