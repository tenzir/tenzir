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
#include "vast/fbs/utils.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice.hpp"
#include "vast/time.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace vast {

void partition_synopsis::shrink() {
  for (auto& [field, synopsis] : field_synopses_) {
    if (!synopsis)
      continue;
    auto shrinked_synopsis = synopsis->shrink();
    if (!shrinked_synopsis)
      continue;
    synopsis.swap(shrinked_synopsis);
  }
}

void partition_synopsis::add(const table_slice& slice,
                             const caf::settings& synopsis_options) {
  auto make_synopsis = [&](const record_field& field) -> synopsis_ptr {
    return has_skip_attribute(field.type)
             ? nullptr
             : factory<synopsis>::make(field.type, synopsis_options);
  };
  for (size_t col = 0; col < slice.columns(); ++col) {
    // Locate the relevant synopsis.
    auto&& layout = slice.layout();
    auto& field = layout.fields[col];
    auto key = qualified_record_field{layout.name(), field};
    auto it = field_synopses_.find(key);
    if (it == field_synopses_.end())
      // Attempt to create a synopsis if we have never seen this key before.
      it = field_synopses_.emplace(std::move(key), make_synopsis(field)).first;
    // If there exists a synopsis for a field, add the entire column.
    if (auto& syn = it->second) {
      for (size_t row = 0; row < slice.rows(); ++row) {
        auto view = slice.at(row, col);
        if (!caf::holds_alternative<caf::none_t>(view))
          syn->add(std::move(view));
      }
    }
  }
}

size_t partition_synopsis::size_bytes() const {
  size_t result = 0;
  for (auto& [field, synopsis] : field_synopses_)
    result += synopsis ? synopsis->size_bytes() : 0ull;
  return result;
}

size_t meta_index::size_bytes() const {
  size_t result = 0;
  for (auto& [id, partition_synopsis] : synopses_)
    result += partition_synopsis.size_bytes();
  return result;
}

void meta_index::add(const uuid& partition, const table_slice& slice) {
  auto& part_syn = synopses_[partition];
  part_syn.add(slice, synopsis_options_);
}

void meta_index::erase(const uuid& partition) {
  synopses_.erase(partition);
}

void meta_index::merge(const uuid& partition, partition_synopsis&& ps) {
  synopses_[partition] = std::move(ps);
}

partition_synopsis& meta_index::at(const uuid& partition) {
  return synopses_.at(partition);
}

void meta_index::replace(const uuid& partition,
                         std::unique_ptr<partition_synopsis> ps) {
  auto it = synopses_.find(partition);
  if (it != synopses_.end()) {
    it->second.field_synopses_.swap(ps->field_synopses_);
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
    if (!memoized_partitions.empty() || synopses_.empty())
      return memoized_partitions;
    memoized_partitions.reserve(synopses_.size());
    std::transform(synopses_.begin(), synopses_.end(),
                   std::back_inserter(memoized_partitions),
                   [](auto& x) { return x.first; });
    std::sort(memoized_partitions.begin(), memoized_partitions.end());
    return memoized_partitions;
  };
  auto f = detail::overload{
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
        if (xs.size() == synopses_.size())
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
      // uses a qualified_record_field to determine whether the synopsis should
      // be queried.
      auto search = [&](auto match) {
        VAST_ASSERT(caf::holds_alternative<data>(x.rhs));
        auto& rhs = caf::get<data>(x.rhs);
        result_type result;
        auto found_matching_synopsis = false;
        for (auto& [part_id, part_syn] : synopses_) {
          VAST_DEBUG(this, "checks", part_id, "for predicate", x);
          for (auto& [field, syn] : part_syn.field_synopses_) {
            if (syn && match(field)) {
              found_matching_synopsis = true;
              auto opt = syn->lookup(x.op, make_view(rhs));
              if (!opt || *opt) {
                VAST_DEBUG(this, "selects", part_id, "at predicate", x);
                result.push_back(part_id);
                break;
              }
            }
          }
        }
        // Re-establish potentially violated invariant.
        std::sort(result.begin(), result.end());
        return found_matching_synopsis ? result : all_partitions();
      };
      auto extract_expr = detail::overload{
        [&](const attribute_extractor& lhs, const data& d) -> result_type {
          if (lhs.attr == atom::timestamp_v) {
            auto pred = [](auto& field) {
              return has_attribute(field.type, "timestamp");
            };
            return search(pred);
          } else if (lhs.attr == atom::type_v) {
            // We don't have to look into the synopses for type queries, just
            // at the layout names.
            result_type result;
            for (auto& [part_id, part_syn] : synopses_) {
              for (auto& pair : part_syn.field_synopses_) {
                // TODO: provide an overload for view of evaluate() so that
                // we can use string_view here. Fortunately type names are
                // short, so we're probably not hitting the allocator due to
                // SSO.
                auto type_name = data{pair.first.layout_name};
                if (evaluate(type_name, x.op, d)) {
                  result.push_back(part_id);
                  break;
                }
              }
            }
            // Re-establish potentially violated invariant.
            std::sort(result.begin(), result.end());
            return result;
          } else if (lhs.attr == atom::field_v) {
            // We don't have to look into the synopses for type queries, just
            // at the layout names.
            result_type result;
            auto s = caf::get_if<std::string>(&d);
            if (!s) {
              VAST_WARNING_ANON("#field meta queries only support string "
                                "comparisons");
            } else {
              for (auto& [part_id, part_syn] : synopses_) {
                bool match = false;
                for (auto& pair : part_syn.field_synopses_) {
                  auto fqn = pair.first.fqn();
                  if (detail::ends_with(fqn, *s)) {
                    match = true;
                    break;
                  }
                }
                if (!negated(x.op) == match)
                  result.push_back(part_id);
              }
            }
            // Re-establish potentially violated invariant.
            std::sort(result.begin(), result.end());
            return result;
          }
          VAST_WARNING(this, "cannot process attribute extractor:", lhs.attr);
          return all_partitions();
        },
        [&](const field_extractor& lhs, const data&) -> result_type {
          auto pred = [&](auto& field) {
            return detail::ends_with(field.fqn(), lhs.field);
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
        },
      };
      return caf::visit(extract_expr, x.lhs, x.rhs);
    },
    [&](caf::none_t) -> result_type {
      VAST_ERROR(this, "received an empty expression");
      VAST_ASSERT(!"invalid expression");
      return all_partitions();
    },
  };
  return caf::visit(f, expr);
}

caf::settings& meta_index::factory_options() {
  return synopsis_options_;
}

caf::expected<flatbuffers::Offset<fbs::partition_synopsis::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const partition_synopsis& x) {
  std::vector<flatbuffers::Offset<fbs::synopsis::v0>> synopses;
  for (auto& [fqf, synopsis] : x.field_synopses_) {
    auto maybe_synopsis = pack(builder, synopsis, fqf);
    if (!maybe_synopsis)
      return maybe_synopsis.error();
    synopses.push_back(*maybe_synopsis);
  }
  auto synopses_vector = builder.CreateVector(synopses);
  fbs::partition_synopsis::v0Builder ps_builder(builder);
  ps_builder.add_synopses(synopses_vector);
  return ps_builder.Finish();
}

caf::error
unpack(const fbs::partition_synopsis::v0& x, partition_synopsis& ps) {
  if (!x.synopses())
    return make_error(ec::format_error, "missing synopses");
  for (auto synopsis : *x.synopses()) {
    if (!synopsis)
      return make_error(ec::format_error, "synopsis is null");
    qualified_record_field qf;
    if (auto error
        = fbs::deserialize_bytes(synopsis->qualified_record_field(), qf))
      return error;
    synopsis_ptr ptr;
    if (auto error = unpack(*synopsis, ptr))
      return error;
    ps.field_synopses_[qf] = std::move(ptr);
  }
  return caf::none;
}

} // namespace vast
