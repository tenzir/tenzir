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

#include "vast/const_table_slice_handle.hpp"
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

// Translates a key to directory.
auto key_to_dir(std::string key, const path& prefix) {
  return prefix / detail::replace_all(std::move(key), ".", path::separator);
}

} // namespace <anonymous>

caf::expected<table_index> make_table_index(path base_dir, record_type layout) {
  caf::error err;
  table_index result{std::move(layout), base_dir};
  result.columns_.resize(table_index::meta_column_count
                         + flat_size(result.layout()));
  return result;
}
// -- constructors, destructors, and assignment operators ----------------------

table_index::~table_index() noexcept {
  if (dirty_)
    flush_to_disk();
}

// -- persistence --------------------------------------------------------------

caf::error table_index::flush_to_disk() {
  // Unless `add` was called at least once there's nothing to flush.
  if (!dirty_)
    return caf::none;
  for (auto& col : columns_) {
    VAST_ASSERT(col != nullptr);
    auto err = col->flush_to_disk();
    if (err)
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

caf::error table_index::add(const const_table_slice_handle& x) {
  VAST_ASSERT(x != nullptr);
  VAST_ASSERT(x->layout() == layout());
  VAST_TRACE(VAST_ARG(x));
  if (dirty_) {
    for (auto& col : columns_) {
      VAST_ASSERT(col != nullptr);
      col->add(x);
    }
    return caf::none;
  }
  auto fun = [&](column_index& col) -> caf::error {
    col.add(x);
    return caf::none;
  };
  auto mk_type = [&] { return make_type_column_index(meta_dir() / "type"); };
  return caf::error::eval(
    [&] {
      // Column 0 is our meta index for the event type.
      return with_meta_column(0, mk_type, fun);
    },
    [&]() -> caf::error {
      // Coluns 1-N are our data fields.
      // Iterate all types of the record.
      size_t i = 0;
      for (auto&& f : record_type::each{layout()}) {
        auto& value_type = f.trace.back()->type;
        if (!has_skip_attribute(layout())) {
          auto fac = [&] {
            VAST_DEBUG("make field indexer at offset", f.offset, "with type",
                       value_type);
            auto dir = key_to_dir(f.key(), data_dir());
            return make_column_index(dir, value_type, i);
          };
          auto err = with_data_column(i, fac, fun);
          if (err)
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
  auto rhs = caf::get_if<data>(&pred.rhs);
  if (!rhs)
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
  VAST_IGNORE_UNUSED(x);
  // We know that the columns vector contains two meta fields: time at index
  // 0 and type at index 1.
  static_assert(table_index::meta_column_count == 1);
  VAST_ASSERT(columns_.size() >= table_index::meta_column_count);
  if (ex.attr == "type") {
    VAST_ASSERT(caf::holds_alternative<std::string>(x));
    auto fac = [&] { return make_type_column_index(meta_dir() / "type"); };
    return with_meta_column(1, fac, [&](column_index& col) {
      return col.lookup(pred);
    });
  } else if (ex.attr == "time") {
    // TODO: reconsider whether we still want to support "&time ..." queries.
    /// We assume column 0 to hold the timestamp.
    VAST_ASSERT(caf::holds_alternative<timestamp>(x));
    if (layout().fields.empty() || layout().fields[0].type != timestamp_type{})
      return ec::invalid_query;
    record_type rs_rec{{"timestamp", timestamp_type{}}};
    type t = rs_rec;
    data_extractor dx{t, vast::offset{0}};
    // Redirect to ordinary data lookup on column 0.
    return lookup_impl(pred, dx, x);
  }
  VAST_WARNING("unsupported attribute:", ex.attr);
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
    VAST_DEBUG("invalid offset for record type", dx.type);
    return bitmap{};
  }
  auto fac = [&] {
    auto dir = key_to_dir(*k, data_dir());
    return make_column_index(dir, *t, *index);
  };
  return with_data_column(*index, fac, [&](column_index& col) {
    return col.lookup(pred);
  });
  return bitmap{};
}

// -- constructors, destructors, and assignment operators ----------------------

table_index::table_index(record_type layout, path base_dir)
  : type_erased_layout_(std::move(layout)),
    base_dir_(std::move(base_dir)),
    dirty_(false) {
  VAST_TRACE(VAST_ARG(type_erased_layout_), VAST_ARG(base_dir_));
}

} // namespace vast
