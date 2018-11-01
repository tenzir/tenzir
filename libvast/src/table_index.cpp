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

#include "vast/table_index.hpp"

#include "vast/detail/overload.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"

#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"

namespace vast {
namespace {

// Translates a key to a directory.
auto key_to_dir(std::string key, const path& prefix) {
  return prefix / detail::replace_all(std::move(key), ".", path::separator);
}

} // namespace <anonymous>

caf::expected<table_index> make_table_index(caf::actor_system& sys,
                                            path base_dir, record_type layout) {
  // Layouts need to be flat.
  VAST_ASSERT(layout.fields.size() == flat_size(layout));
  VAST_TRACE(VAST_ARG(base_dir), VAST_ARG(layout));
  caf::error err;
  table_index result{sys, std::move(layout), base_dir};
  if (auto err = result.init())
    return err;
  return result;
}

// -- constructors, destructors, and assignment operators ----------------------

table_index::table_index(caf::actor_system& sys) : sys_(sys) {
  // nop
}

table_index::~table_index() noexcept {
  if (dirty_)
    flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error table_index::init() {
  VAST_TRACE("");
  columns_.resize(layout().fields.size());
  auto filename = base_dir_ / "row_ids";
  if (exists(filename))
    return load(sys_, filename, row_ids_);
  return caf::none;
}

caf::error table_index::flush_to_disk() {
  // Unless `add` was called at least once there's nothing to flush.
  VAST_TRACE("");
  if (!dirty_)
    return caf::none;
  if (auto err = save(sys_, base_dir_ / "row_ids", row_ids_))
    return err;
  for (auto& col : columns_) {
    VAST_ASSERT(col != nullptr);
    if (auto err = col->flush_to_disk())
      return err;
  }
  dirty_ = false;
  return caf::none;
}

/// -- properties --------------------------------------------------------------

column_index& table_index::at(size_t column_index) {
  VAST_ASSERT(column_index < columns_.size());
  return *columns_[column_index];
}

column_index* table_index::by_name(std::string_view column_name) {
  // TODO: support string_view in path's operator /
  auto fname = base_dir_ / std::string{column_name};
  auto pred = [&](const column_index_ptr& ptr) {
    return ptr->filename() == fname;
  };
  auto i = std::find_if(columns_.begin(), columns_.end(), pred);
  return i != columns_.end() ? i->get() : nullptr;
}

caf::error table_index::add(const table_slice_ptr& x) {
  VAST_ASSERT(x != nullptr);
  VAST_ASSERT(x->layout() == layout());
  VAST_TRACE(VAST_ARG(x));
  // Store IDs of the new rows.
  auto first = x->offset();
  auto last = x->offset() + x->rows();
  VAST_ASSERT(first < last);
  VAST_ASSERT(first >= row_ids_.size());
  row_ids_.append_bits(false, first - row_ids_.size());
  row_ids_.append_bits(true, last - first);
  // Iterate columns directly if all columns are present in memory.
  if (dirty_) {
    for (auto& col : columns_) {
      VAST_ASSERT(col != nullptr);
      col->add(x);
    }
    return caf::none;
  }
  // Create columns on-the-fly.
  auto fun = [&](column_index& col) -> caf::error {
    col.add(x);
    return caf::none;
  };
  return caf::error::eval(
    [&]() -> caf::error {
      // Iterate all types of the record.
      size_t i = 0;
      for (auto&& f : record_type::each{layout()}) {
        auto& value_type = f.trace.back()->type;
        if (!has_skip_attribute(layout())) {
          auto fac = [&] {
            VAST_DEBUG(this, "makes field indexer at offset", f.offset,
                       "with type", value_type);
            auto dir = key_to_dir(f.key(), data_dir());
            return make_column_index(sys_, dir, value_type, i);
          };
          if (auto err = with_column(i, fac, fun))
            return err;
          ++i;
        }
      }
      return caf::none;
    },
    [&]() -> caf::error {
      dirty_ = true;
      return caf::none;
    });
}

path table_index::meta_dir() const {
  return base_dir_ / "meta";
}

path table_index::data_dir() const {
  return base_dir_ / "data";
}

caf::expected<bitmap> table_index::lookup(const predicate& pred) {
  VAST_TRACE(VAST_ARG(pred));
  // For now, we require that the predicate is part of a normalized expression,
  // i.e., LHS is an extractor and RHS is a data.
  if (!caf::holds_alternative<data>(pred.rhs))
    return ec::invalid_query;
  // Specialize the predicate for the type.
  auto resolved = type_resolver{type_erased_layout_}(pred);
  if (!resolved)
    return std::move(resolved.error());
  return lookup_impl(*resolved);
}

caf::expected<bitmap> table_index::lookup(const expression& expr) {
  VAST_TRACE(VAST_ARG(expr));
  // Specialize the expression for the type.
  type_resolver resolver{type_erased_layout_};
  auto resolved = caf::visit(resolver, expr);
  if (!resolved)
    return std::move(resolved.error());
  return lookup_impl(*resolved);
}

caf::expected<bitmap> table_index::lookup_impl(const expression& expr) {
  VAST_TRACE(VAST_ARG(expr));
  return caf::visit(
    detail::overload(
      [&](const auto& seq) -> expected<bitmap> {
        static constexpr bool is_disjunction
          = std::is_same_v<decltype(seq), const disjunction&>;
        static_assert(is_disjunction
                      || std::is_same_v<decltype(seq), const conjunction&>);
        VAST_ASSERT(!seq.empty());
        bitmap result;
        {
          auto r0 = lookup_impl(seq.front());
          if (!r0)
            return r0.error();
          result = std::move(*r0);
        }
        for (auto i = seq.begin() + 1; i != seq.end(); ++i) {
          // short-circuit
          if constexpr (is_disjunction) {
            if (all<1>(result))
              return result;
          } else {
            if (all<0>(result))
              return result;
          }
          auto sub_result = lookup_impl(*i);
          if (!sub_result)
            return sub_result.error();
          if constexpr (is_disjunction)
            result |= *sub_result;
          else
            result &= *sub_result;
        }
        return result;
      },
      [&](const negation& neg) {
        auto result = lookup_impl(neg.expr());
        if (result)
          result->flip();
        return result;
      },
      [&](const predicate& p) {
        return visit(
          detail::overload(
            [&](const attribute_extractor& ex, const data& x) {
              return lookup_impl(p, ex, x);
            },
            [&](const data_extractor& dx, const data& x) {
              return lookup_impl(p, dx, x);
            },
            [&](const auto&, const auto&) -> expected<bitmap> {
              // Ignore unexpected lhs/rhs combinations.
              return bitmap{};
            }
          ),
          p.lhs, p.rhs);
      },
      [&](const caf::none_t&) -> expected<bitmap> {
        return bitmap{};
      }),
    expr);
}

caf::expected<bitmap> table_index::lookup_impl(const predicate& pred,
                                               const attribute_extractor& ex,
                                               const data& x) {
  VAST_TRACE(VAST_ARG(pred), VAST_ARG(ex), VAST_ARG(x));
  if (ex.attr == "type") {
    VAST_ASSERT(caf::holds_alternative<std::string>(x));
    // No hits if the queries name doesn't match our type.
    if (layout().name() != caf::get<std::string>(x))
      return ids{};
    // Otherwise all rows match.
    return row_ids_;
  } else if (ex.attr == "time") {
    // TODO: reconsider whether we still want to support "&time ..." queries.
    VAST_ASSERT(caf::holds_alternative<timestamp>(x));
    if (layout().fields.empty() || layout().fields[0].type != timestamp_type{})
      return ec::invalid_query;
    record_type rs_rec{{"timestamp", timestamp_type{}}};
    type t = rs_rec;
    data_extractor dx{t, vast::offset{0}};
    // Redirect to ordinary data lookup on column 0.
    return lookup_impl(pred, dx, x);
  }
  VAST_WARNING(this, "got unsupported attribute:", ex.attr);
  return ec::invalid_query;
}

caf::expected<bitmap> table_index::lookup_impl(const predicate& pred,
                                               const data_extractor& dx,
                                               const data& x) {
  VAST_TRACE(VAST_ARG(pred), VAST_ARG(dx), VAST_ARG(x));
  VAST_IGNORE_UNUSED(x);
  if (dx.offset.empty()) {
    return bitmap{};
  }
  auto r = caf::get<record_type>(dx.type);
  auto k = r.resolve(dx.offset);
  VAST_ASSERT(k);
  auto t = r.at(dx.offset);
  VAST_ASSERT(t);
  auto index = r.flat_index_at(dx.offset);
  if (!index) {
    VAST_DEBUG(this, "got invalid offset for record type", dx.type);
    return bitmap{};
  }
  auto fac = [&] {
    auto dir = key_to_dir(*k, data_dir());
    return make_column_index(sys_, dir, *t, *index);
  };
  return with_column(*index, fac, [&](column_index& col) {
    return col.lookup(pred);
  });
  return bitmap{};
}

// -- constructors, destructors, and assignment operators ----------------------

table_index::table_index(caf::actor_system& sys, record_type layout,
                         path base_dir)
  : type_erased_layout_(std::move(layout)),
    base_dir_(std::move(base_dir)),
    dirty_(false),
    sys_(sys) {
  VAST_TRACE(VAST_ARG(type_erased_layout_), VAST_ARG(base_dir_));
}

} // namespace vast
