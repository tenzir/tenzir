//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/bitmap_algorithms.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/passthrough.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/type.hpp"

#include <arrow/record_batch.h>

#include <cstddef>
#include <regex>
#include <span>

namespace tenzir {

namespace {

template <class LhsView, class Rhs>
inline constexpr auto requires_stdcmp
  = std::is_integral_v<LhsView> && std::is_integral_v<Rhs>
    && !std::is_same_v<LhsView, Rhs> && !detail::is_any_v<bool, LhsView, Rhs>;

template <relational_operator Op>
struct cell_evaluator;

template <>
struct cell_evaluator<relational_operator::equal> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  template <class LhsView, class Rhs>
    requires requires(const LhsView& lhs, const Rhs& rhs) {
      { lhs == rhs } -> std::same_as<bool>;
    }
  static bool evaluate(LhsView lhs, const Rhs& rhs) noexcept {
    if constexpr (requires_stdcmp<LhsView, Rhs>)
      return std::cmp_equal(lhs, rhs);
    else
      return lhs == rhs;
  }

  static bool evaluate(std::string_view lhs, const pattern& rhs) noexcept {
    return rhs.match(lhs);
  }

  static bool evaluate(view<pattern> lhs, const std::string& rhs) noexcept {
    return evaluate(rhs, materialize(lhs));
  }

  static bool evaluate(view<ip> lhs, const subnet& rhs) noexcept {
    return rhs.contains(lhs);
  }

  static bool evaluate(view<subnet> lhs, const ip& rhs) noexcept {
    return evaluate(rhs, materialize(lhs));
  }
};

template <>
struct cell_evaluator<relational_operator::not_equal> {
  static bool evaluate(auto lhs, const auto& rhs) noexcept {
    return !cell_evaluator<relational_operator::equal>::evaluate(lhs, rhs);
  }
};

template <>
struct cell_evaluator<relational_operator::less> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  template <class LhsView, class Rhs>
    requires requires(const LhsView& lhs, const Rhs& rhs) {
      { lhs < rhs } -> std::same_as<bool>;
    }
  static bool evaluate(LhsView lhs, const Rhs& rhs) noexcept {
    if constexpr (requires_stdcmp<LhsView, Rhs>)
      return std::cmp_less(lhs, rhs);
    else
      return lhs < rhs;
  }
};

template <>
struct cell_evaluator<relational_operator::less_equal> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  template <class LhsView, class Rhs>
    requires requires(const LhsView& lhs, const Rhs& rhs) {
      { lhs <= rhs } -> std::same_as<bool>;
    }
  static bool evaluate(LhsView lhs, const Rhs& rhs) noexcept {
    if constexpr (requires_stdcmp<LhsView, Rhs>)
      return std::cmp_less_equal(lhs, rhs);
    else
      return lhs <= rhs;
  }
};

template <>
struct cell_evaluator<relational_operator::greater> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  template <class LhsView, class Rhs>
    requires requires(const LhsView& lhs, const Rhs& rhs) {
      { lhs > rhs } -> std::same_as<bool>;
    }
  static bool evaluate(LhsView lhs, const Rhs& rhs) noexcept {
    if constexpr (requires_stdcmp<LhsView, Rhs>)
      return std::cmp_greater(lhs, rhs);
    else
      return lhs > rhs;
  }
};

template <>
struct cell_evaluator<relational_operator::greater_equal> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  template <class LhsView, class Rhs>
    requires requires(const LhsView& lhs, const Rhs& rhs) {
      { lhs >= rhs } -> std::same_as<bool>;
    }
  static bool evaluate(LhsView lhs, const Rhs& rhs) noexcept {
    if constexpr (requires_stdcmp<LhsView, Rhs>)
      return std::cmp_greater_equal(lhs, rhs);
    else
      return lhs >= rhs;
  }
};

template <>
struct cell_evaluator<relational_operator::in> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  static bool evaluate(view<std::string> lhs, const std::string& rhs) noexcept {
    return rhs.find(lhs) != view<std::string>::npos;
  }

  static bool evaluate(view<std::string> lhs, const pattern& rhs) noexcept {
    return rhs.search(lhs);
  }

  static bool evaluate(view<ip> lhs, const subnet& rhs) noexcept {
    return rhs.contains(lhs);
  }

  static bool evaluate(view<subnet> lhs, const subnet& rhs) noexcept {
    return rhs.contains(lhs);
  }

  static bool evaluate(auto lhs, const list& rhs) noexcept {
    return std::any_of(rhs.begin(), rhs.end(), [lhs](const data& element) {
      return caf::visit(
        [lhs](const auto& element) noexcept {
          return cell_evaluator<relational_operator::equal>::evaluate(lhs,
                                                                      element);
        },
        element);
    });
  }
};

template <>
struct cell_evaluator<relational_operator::not_in> {
  static bool evaluate(auto lhs, const auto& rhs) noexcept {
    return !cell_evaluator<relational_operator::in>::evaluate(lhs, rhs);
  }
};

template <>
struct cell_evaluator<relational_operator::ni> {
  static bool evaluate(auto, const auto&) noexcept {
    return false;
  }

  static bool evaluate(view<std::string> lhs, const std::string& rhs) noexcept {
    return lhs.find(rhs) != view<std::string>::npos;
  }

  static bool evaluate(view<subnet> lhs, const ip& rhs) noexcept {
    return lhs.contains(rhs);
  }

  static bool evaluate(view<subnet> lhs, const subnet& rhs) noexcept {
    return lhs.contains(rhs);
  }

  static bool evaluate(view<list> lhs, const auto& rhs) noexcept {
    return std::any_of(lhs.begin(), lhs.end(), [rhs](const auto& element) {
      return caf::visit(
        [rhs](const auto& element) noexcept {
          return cell_evaluator<relational_operator::equal>::evaluate(element,
                                                                      rhs);
        },
        element);
    });
  }
};

template <>
struct cell_evaluator<relational_operator::not_ni> {
  static bool evaluate(auto lhs, const auto& rhs) noexcept {
    return !cell_evaluator<relational_operator::ni>::evaluate(lhs, rhs);
  }
};

// The default implementation for the column evaluator that dispatches to the
// cell evaluator for every relevant row.
template <relational_operator Op, concrete_type LhsType, class Rhs>
struct column_evaluator {
  static ids evaluate(LhsType type, id offset, const arrow::Array& array,
                      const Rhs& rhs, const ids& selection) noexcept {
    ids result{};
    for (auto id : select(selection)) {
      TENZIR_ASSERT(id >= offset);
      const auto row = detail::narrow_cast<int64_t>(id - offset);
      // TODO: Instead of this in the loop, do selection &= array.null_bitmap
      // outside of it.
      if (array.IsNull(row))
        continue;
      result.append(false, id - result.size());
      result.append(
        cell_evaluator<Op>::evaluate(value_at(type, array, row), rhs), 1u);
    }
    result.append(false, offset + array.length() - result.size());
    return result;
  }
};

// Special-case equal operations with null.
template <concrete_type LhsType>
struct column_evaluator<relational_operator::equal, LhsType, caf::none_t> {
  static ids
  evaluate([[maybe_unused]] LhsType type, id offset, const arrow::Array& array,
           [[maybe_unused]] caf::none_t rhs, const ids& selection) noexcept {
    // TODO: This entire loop semantically is just selection &
    // ~array.null_bitmap, but we cannot do bitwise operations with Arrow's
    // bitmaps yet.
    ids result{};
    for (auto id : select(selection)) {
      TENZIR_ASSERT(id >= offset);
      const auto row = detail::narrow_cast<int64_t>(id - offset);
      if (!array.IsNull(row))
        continue;
      result.append(false, id - result.size());
      result.append(true, 1u);
    }
    result.append(false, offset + array.length() - result.size());
    return result;
  }
};

// Special-case not-equal operations with null.
template <concrete_type LhsType>
struct column_evaluator<relational_operator::not_equal, LhsType, caf::none_t> {
  static ids
  evaluate([[maybe_unused]] LhsType type, id offset, const arrow::Array& array,
           [[maybe_unused]] caf::none_t rhs, const ids& selection) noexcept {
    // TODO: This entire loop semantically is just selection &
    // array.null_bitmap, but we cannot do bitwise operations with Arrow's
    // bitmaps yet.
    ids result{};
    for (auto id : select(selection)) {
      TENZIR_ASSERT(id >= offset);
      const auto row = detail::narrow_cast<int64_t>(id - offset);
      if (array.IsNull(row))
        continue;
      result.append(false, id - result.size());
      result.append(true, 1u);
    }
    result.append(false, offset + array.length() - result.size());
    return result;
  }
};

// All operations comparing with null except for equal and not-equal always
// yield no results.
template <relational_operator Op, concrete_type LhsType>
struct column_evaluator<Op, LhsType, caf::none_t> {
  static ids
  evaluate([[maybe_unused]] LhsType type, id offset, const arrow::Array& array,
           [[maybe_unused]] caf::none_t rhs,
           [[maybe_unused]] const ids& selection) noexcept {
    return ids{offset + array.length(), false};
  }
};

// For operations comparing enumeration arrays with a string view we want to
// first convert the string view into its underlying integral representation,
// and then dispatch to that column evaluator.
template <relational_operator Op>
struct column_evaluator<Op, enumeration_type, std::string> {
  static ids
  evaluate(enumeration_type type, id offset, const arrow::Array& array,
           const std::string& rhs, const ids& selection) noexcept {
    if (auto key = type.resolve(rhs)) {
      auto rhs_internal = detail::narrow_cast<view<enumeration>>(*key);
      return column_evaluator<Op, enumeration_type, view<enumeration>>::evaluate(
        type, offset, array, rhs_internal, selection);
    }
    return ids{offset + array.length(), false};
  }
};

// A utility function for evaluating meta extractors in predicates. This is
// always a yes or no question per batch, so the function does not have to deal
// with bitmaps at all.
bool evaluate_meta_extractor(const table_slice& slice,
                             const meta_extractor& lhs, relational_operator op,
                             const data& rhs) {
  switch (lhs.kind) {
    case meta_extractor::kind::schema: {
      switch (op) {
#define TENZIR_EVAL_DISPATCH(op)                                               \
  case relational_operator::op: {                                              \
    auto f = [&](const auto& rhs) noexcept {                                   \
      return cell_evaluator<relational_operator::op>::evaluate(                \
        slice.schema().name(), rhs);                                           \
    };                                                                         \
    return caf::visit(f, rhs);                                                 \
  }
        TENZIR_EVAL_DISPATCH(equal);
        TENZIR_EVAL_DISPATCH(not_equal);
        TENZIR_EVAL_DISPATCH(in);
        TENZIR_EVAL_DISPATCH(less);
        TENZIR_EVAL_DISPATCH(not_in);
        TENZIR_EVAL_DISPATCH(greater);
        TENZIR_EVAL_DISPATCH(greater_equal);
        TENZIR_EVAL_DISPATCH(less_equal);
        TENZIR_EVAL_DISPATCH(ni);
        TENZIR_EVAL_DISPATCH(not_ni);
#undef TENZIR_EVAL_DISPATCH
      }
      TENZIR_UNREACHABLE();
    }
    case meta_extractor::kind::schema_id: {
      switch (op) {
#define TENZIR_EVAL_DISPATCH(op)                                               \
  case relational_operator::op: {                                              \
    auto f = [&](const auto& rhs) noexcept {                                   \
      return cell_evaluator<relational_operator::op>::evaluate(                \
        slice.schema().make_fingerprint(), rhs);                               \
    };                                                                         \
    return caf::visit(f, rhs);                                                 \
  }
        TENZIR_EVAL_DISPATCH(equal);
        TENZIR_EVAL_DISPATCH(not_equal);
        TENZIR_EVAL_DISPATCH(in);
        TENZIR_EVAL_DISPATCH(less);
        TENZIR_EVAL_DISPATCH(not_in);
        TENZIR_EVAL_DISPATCH(greater);
        TENZIR_EVAL_DISPATCH(greater_equal);
        TENZIR_EVAL_DISPATCH(less_equal);
        TENZIR_EVAL_DISPATCH(ni);
        TENZIR_EVAL_DISPATCH(not_ni);
#undef TENZIR_EVAL_DISPATCH
      }
      TENZIR_UNREACHABLE();
    }
    case meta_extractor::kind::import_time: {
      switch (op) {
#define TENZIR_EVAL_DISPATCH(op)                                               \
  case relational_operator::op: {                                              \
    auto f = [&](const auto& rhs) noexcept {                                   \
      return cell_evaluator<relational_operator::op>::evaluate(                \
        slice.import_time(), rhs);                                             \
    };                                                                         \
    return caf::visit(f, rhs);                                                 \
  }
        TENZIR_EVAL_DISPATCH(equal);
        TENZIR_EVAL_DISPATCH(not_equal);
        TENZIR_EVAL_DISPATCH(in);
        TENZIR_EVAL_DISPATCH(less);
        TENZIR_EVAL_DISPATCH(not_in);
        TENZIR_EVAL_DISPATCH(greater);
        TENZIR_EVAL_DISPATCH(greater_equal);
        TENZIR_EVAL_DISPATCH(less_equal);
        TENZIR_EVAL_DISPATCH(ni);
        TENZIR_EVAL_DISPATCH(not_ni);
#undef TENZIR_EVAL_DISPATCH
      }
      TENZIR_UNREACHABLE();
    }
    case meta_extractor::kind::internal: {
      switch (op) {
#define TENZIR_EVAL_DISPATCH(op)                                               \
  case relational_operator::op: {                                              \
    auto f = [&](const auto& rhs) noexcept {                                   \
      return cell_evaluator<relational_operator::op>::evaluate(                \
        slice.schema().attribute("internal").has_value(), rhs);                \
    };                                                                         \
    return caf::visit(f, rhs);                                                 \
  }
        TENZIR_EVAL_DISPATCH(equal);
        TENZIR_EVAL_DISPATCH(not_equal);
        TENZIR_EVAL_DISPATCH(in);
        TENZIR_EVAL_DISPATCH(less);
        TENZIR_EVAL_DISPATCH(not_in);
        TENZIR_EVAL_DISPATCH(greater);
        TENZIR_EVAL_DISPATCH(greater_equal);
        TENZIR_EVAL_DISPATCH(less_equal);
        TENZIR_EVAL_DISPATCH(ni);
        TENZIR_EVAL_DISPATCH(not_ni);
#undef TENZIR_EVAL_DISPATCH
      }
      TENZIR_UNREACHABLE();
    }
  }
  __builtin_unreachable();
}

} // namespace

// Expression evaluation takes place in multiple resolution steps:
// 1. Normalize the selection bitmap from the dense index result to the length
//    of the batch + offset.
// 2. Determine whether the expression is empty, a connective of some sort, or
//    a predicate. For connectives, resolve them recursively and combine the
//    resulting bitmaps accordingly.
// 3. Evaluate predicates:
//    a) If it's a meta extractor, operate on the batch metadata. In case of a
//       match, the selection bitmap is the very result.
//    b) If it's a data predicate, access the desired array, and lift the
//       resolved types for both sides of the predicate into a compile-time
//       context for the column evaluator.
// 4. The column evaluator has specialization based on the three-tuple of lhs
//    type, relational operator, and rhs view. The generic fall back case
//    iterates over all fields per the selection bitmap to do the evaluation
//    using the cell evaluator, which can be specialized per relational
//    operator.
ids evaluate(const expression& expr, const table_slice& slice,
             const ids& hints) {
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  const auto num_rows = slice.rows();
  const auto evaluate_predicate = detail::overload{
    [](const auto&, relational_operator, const auto&, const ids&) -> ids {
      die("predicates must be normalized and bound for evaluation");
    },
    [&](const meta_extractor& lhs, relational_operator op, const data& rhs,
        ids selection) -> ids {
      // If no bit in the selection is set we have no results, but we can avoid
      // an allocation by simply returning the already empty selection.
      if (!any(selection))
        return selection;
      if (evaluate_meta_extractor(slice, lhs, op, rhs))
        return selection;
      return ids{offset + num_rows, false};
    },
    [&](const data_extractor& lhs, relational_operator op, const data& rhs,
        const ids& selection) -> ids {
      if (!any(selection))
        return ids{offset + num_rows, false};
      const auto index
        = as<record_type>(slice.schema()).resolve_flat_index(lhs.column);
      const auto type_and_array = index.get(slice);
      TENZIR_ASSERT(type_and_array.second);
      switch (op) {
#define TENZIR_EVAL_DISPATCH(op)                                               \
  case relational_operator::op: {                                              \
    auto f = [&]<concrete_type Type, class Rhs>(                               \
               Type type, const Rhs& rhs) noexcept -> ids {                    \
      return column_evaluator<relational_operator::op, Type, Rhs>::evaluate(   \
        type, offset, *type_and_array.second, rhs, selection);                 \
    };                                                                         \
    return caf::visit(f, type_and_array.first, rhs);                           \
  }
        TENZIR_EVAL_DISPATCH(equal);
        TENZIR_EVAL_DISPATCH(not_equal);
        TENZIR_EVAL_DISPATCH(in);
        TENZIR_EVAL_DISPATCH(less);
        TENZIR_EVAL_DISPATCH(not_in);
        TENZIR_EVAL_DISPATCH(greater);
        TENZIR_EVAL_DISPATCH(greater_equal);
        TENZIR_EVAL_DISPATCH(less_equal);
        TENZIR_EVAL_DISPATCH(ni);
        TENZIR_EVAL_DISPATCH(not_ni);
#undef TENZIR_EVAL_DISPATCH
      }
      TENZIR_UNREACHABLE();
    },
  };
  const auto evaluate_expression
    = [&](const auto& self, const expression& expr, ids selection) -> ids {
    const auto evaluate_expression_impl = detail::overload{
      [&](const caf::none_t&, const ids&) {
        return ids{offset + num_rows, false};
      },
      [&](const negation& negation, const ids& selection) {
        // For negations we want to return a bitmap that has 1s in places where
        // the selection had 1s and the nested expression evaluation returned
        // 0s. The opposite case where the selection has 0s and the nested
        // expression evaluation returns 1s cannot exist (this is a precondition
        // violation), so we can simply XOR the bitmaps to do the negation.
        return selection ^ self(self, negation.expr(), selection);
      },
      [&](const conjunction& conjunction, ids selection) {
        for (const auto& connective : conjunction) {
          if (!any(selection))
            return selection;
          selection = self(self, connective, std::move(selection));
        }
        return selection;
      },
      [&](const disjunction& disjunction, const ids& selection) {
        auto mask = selection;
        for (const auto& connective : disjunction) {
          if (!any(mask))
            return selection;
          mask &= ~self(self, connective, mask);
        }
        return selection & ~mask;
      },
      [&](const predicate& predicate, const ids& selection) -> ids {
        return caf::visit(evaluate_predicate, predicate.lhs,
                          detail::passthrough(predicate.op), predicate.rhs,
                          detail::passthrough(selection));
      },
    };
    return caf::visit(evaluate_expression_impl, expr,
                      detail::passthrough(selection));
  };
  auto selection = ids{};
  selection.append(false, offset);
  if (hints.empty()) {
    selection.append(true, num_rows);
  } else {
    for (auto hint : select(hints)) {
      if (hint < offset)
        continue;
      if (hint >= offset + num_rows)
        break;
      selection.append(false, hint - selection.size());
      selection.append<true>();
    }
    selection.append(false, offset + num_rows - selection.size());
  }
  TENZIR_ASSERT(selection.size() == offset + num_rows);
  auto result
    = evaluate_expression(evaluate_expression, expr, std::move(selection));
  TENZIR_ASSERT(result.size() == offset + num_rows);
  return result;
}

} // namespace tenzir
