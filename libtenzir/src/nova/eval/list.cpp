#include "tenzir/location.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

/// An all-null-lists `Array<List>` of the given `length`: every row is
/// present but empty.
auto make_empty_lists(storage::Index length) -> Array<List> {
  return Array<List>{
    storage::DataOwner<storage::Span[]>::make_value(length,
                                                    storage::Span{0, 0}),
    ArrayBuilder<Data>{}.finish(),
  };
}

/// The row-invariant shape of a single spread's contribution to a list
/// literal, resolved once (not per row).
struct SpreadShape {
  /// Set when the spread's value is (or contains, via a union) a list
  /// column; absent otherwise (null spread, or type mismatch).
  Option<Array<List>> list;
};

/// Resolves a spread expression's value into a `SpreadShape`, emitting a
/// warning if the value is neither a list, a union with a list alternative,
/// nor null.
auto resolve_spread(EvalFrame frame, const ast::spread& spread,
                    Array<Data> const& value) -> SpreadShape {
  if (auto list = value.try_as<List>()) {
    return {std::move(*list)};
  }
  if (auto* u = try_as<UnionArray>(value)) {
    auto alt = u->get_alternative<List>();
    if (frame.mask()
          .and_not(u->alternative_mask<List>())
          .and_not(u->alternative_mask<Null>())
          .any()) {
      diagnostic::warning("expected `list`").primary(spread.expr).emit(frame);
    }
    if (alt) {
      return {std::move(alt->data)};
    }
    return {};
  }
  if (value.try_as<Null>()) {
    return {};
  }
  diagnostic::warning("expected `list`").primary(spread.expr).emit(frame);
  return {};
}

} // namespace

auto _::EvalRun::eval(const ast::list& x, EvalFrame frame) -> Array<Data> {
  auto const& mask = frame.mask();
  auto const length = frame.length();

  if (x.items.empty()) {
    return Array<Data>{make_empty_lists(length)};
  }

  if (x.items.size() == 1) {
    if (auto const* item = try_as<ast::expression>(x.items.front())) {
      // Fast path: a single non-spread item becomes one list per row,
      // wrapping the evaluated array without copying its data.
      auto value = frame.eval(*item);
      auto spans
        = storage::DataOwner<storage::Span[]>::make_uninitialized(length);
      for (auto i = storage::Index{0}; i < length; ++i) {
        spans.emplace_back(i, i + 1);
      }
      return Array<Data>{Array<List>{spans.finish(), std::move(value)}};
    }
    // Fast path: a single spread becomes the spreadee's list array reused
    // as-is, with no per-row work at all.
    auto const& spread = as<ast::spread>(x.items.front());
    auto value = frame.eval(spread.expr);
    auto shape = resolve_spread(frame, spread, value);
    if (shape.list) {
      return Array<Data>{std::move(*shape.list)};
    }
    return Array<Data>{make_empty_lists(length)};
  }

  // General path: evaluate every item up front, then assemble the result
  // row by row in source order, since list element order is significant.
  struct EvaluatedItem {
    bool is_spread = false;
    Array<Data> value;
    SpreadShape spread_shape;
  };
  auto items = std::vector<EvaluatedItem>{};
  items.reserve(x.items.size());
  for (auto const& item : x.items) {
    if (auto const* spread = try_as<ast::spread>(item)) {
      auto value = frame.eval(spread->expr);
      auto shape = resolve_spread(frame, *spread, value);
      items.push_back({.is_spread = true,
                       .value = std::move(value),
                       .spread_shape = std::move(shape)});
      continue;
    }
    auto const& expr = as<ast::expression>(item);
    items.push_back(
      {.is_spread = false, .value = frame.eval(expr), .spread_shape = {}});
  }

  auto builder = ArrayBuilder<List>{};
  for (auto i = storage::Index{0}; i < length; ++i) {
    if (not mask.get(i)) {
      builder.skip();
      continue;
    }
    auto lb = builder.list();
    for (auto const& item : items) {
      if (not item.is_spread) {
        // Every requested row holds a materialized value, and `append_row`
        // renders an explicit `Null` as a null element.
        append_row(lb, item.value.get(i));
        continue;
      }
      if (not item.spread_shape.list) {
        continue;
      }
      auto const& list = *item.spread_shape.list;
      if (i >= list.length()) {
        continue;
      }
      for (auto elem : list.get(i)) {
        append_row(lb, elem);
      }
    }
  }
  return Array<Data>{builder.finish()};
}

} // namespace tenzir::nova
