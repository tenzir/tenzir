//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series.hpp"
#include "tenzir/tql2/arrow_utils.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval_impl.hpp"

#include <arrow/compute/api_scalar.h>

#include <type_traits>

// TODO: This file takes very long to compile. Consider splitting it up even more.

namespace tenzir {

namespace {

template <class T>
concept numeric_type
  = std::same_as<T, int64_type> || std::same_as<T, uint64_type>
    || std::same_as<T, double_type>;

template <ast::binary_op Op, concrete_type L, concrete_type R>
struct EvalBinOp;

template <numeric_type L, numeric_type R>
struct EvalBinOp<ast::binary_op::add, L, R> {
  // double + (u)int -> double
  // uint + int -> int? uint?

  static auto
  eval(const type_to_arrow_array_t<L>& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::Array> {
    // if constexpr (std::same_as<L, double_type>
    //               || std::same_as<R, double_type>) {
    //   // double
    // } else {
    //   // if both uint -> uint
    //   // otherwise int?
    // }
    // TODO: Do we actually want to use this?
    // For example, range error leads to no result at all.
    auto opts = arrow::compute::ArithmeticOptions{};
    opts.check_overflow = true;
    auto res = arrow::compute::Add(l, r, opts).ValueOrDie();
    TENZIR_ASSERT(res.is_array());
    return res.make_array();
  }
};

template <>
struct EvalBinOp<ast::binary_op::add, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r)
    -> std::shared_ptr<arrow::StringArray> {
    auto b = arrow::StringBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        b.UnsafeAppendNull();
        continue;
      }
      auto lv = l.GetView(i);
      auto rv = r.GetView(i);
      check(b.Append(lv));
      check(b.ExtendCurrent(rv));
    }
    return finish(b);
  }
};

template <numeric_type L, numeric_type R>
struct EvalBinOp<ast::binary_op::mul, L, R> {
  static auto
  eval(const type_to_arrow_array_t<L>& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::Array> {
    auto opts = arrow::compute::ArithmeticOptions{};
    opts.check_overflow = true;
    auto res = arrow::compute::Multiply(l, r, opts).ValueOrDie();
    TENZIR_ASSERT(res.is_array());
    return res.make_array();
  }
};

// TODO: Generalize.
template <numeric_type T>
struct EvalBinOp<ast::binary_op::eq, T, T> {
  static auto
  eval(const type_to_arrow_array_t<T>& l, const type_to_arrow_array_t<T>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      if (ln && rn) {
        b.UnsafeAppend(true);
      } else if (ln != rn) {
        b.UnsafeAppend(false);
      } else {
        auto lv = l.Value(i);
        auto rv = r.Value(i);
        b.UnsafeAppend(lv == rv);
      }
    }
    return finish(b);
  }
};

template <numeric_type T>
struct EvalBinOp<ast::binary_op::gt, T, T> {
  static auto
  eval(const type_to_arrow_array_t<T>& l, const type_to_arrow_array_t<T>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      if (ln || rn) {
        b.UnsafeAppendNull();
      } else {
        auto lv = l.Value(i);
        auto rv = r.Value(i);
        b.UnsafeAppend(lv > rv);
      }
    }
    return finish(b);
  }
};

// TODO: We probably don't want this.
template <>
struct EvalBinOp<ast::binary_op::and_, bool_type, bool_type> {
  static auto eval(const arrow::BooleanArray& l, const arrow::BooleanArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: Bad.
    auto has_null = l.null_bitmap() || r.null_bitmap();
    auto has_offset = l.offset() != 0 || r.offset() != 0;
    if (not has_null && not has_offset) {
      auto buffer = check(arrow::AllocateBitmap(l.length()));
      auto size = (l.length() + 7) / 8;
      TENZIR_ASSERT(l.values()->size() >= size);
      TENZIR_ASSERT(r.values()->size() >= size);
      auto l_ptr = l.values()->data();
      auto r_ptr = r.values()->data();
      auto o_ptr = buffer->mutable_data();
      for (auto i = int64_t{0}; i < size; ++i) {
        o_ptr[i] = l_ptr[i] & r_ptr[i];
      }
      return std::make_shared<arrow::BooleanArray>(l.length(),
                                                   std::move(buffer));
    }
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto lv = l.Value(i);
      auto rv = r.Value(i);
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      auto lt = not ln && lv;
      auto lf = not ln && not lv;
      auto rt = not rn && rv;
      auto rf = not rn && not rv;
      if (lt && rt) {
        b.UnsafeAppend(true);
      } else if (lf || rf) {
        b.UnsafeAppend(false);
      } else {
        b.UnsafeAppendNull();
      }
    }
    return finish(b);
  }
};

template <ast::binary_op Op, concrete_type L>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    constexpr auto invert = Op == ast::binary_op::neq;
    // TODO: This is bad.
    TENZIR_UNUSED(r);
    auto buffer = check(arrow::AllocateBitmap(l.length()));
    auto& null_bitmap = l.null_bitmap();
    if (not null_bitmap) {
      // All non-null, except if `null_type`.
      auto value = (std::same_as<L, null_type> != invert) ? 0xFF : 0x00;
      std::memset(buffer->mutable_data(), value, buffer->size());
    } else {
      TENZIR_ASSERT(buffer->size() <= null_bitmap->size());
      auto buffer_ptr = buffer->mutable_data();
      auto null_ptr = null_bitmap->data();
      auto length = detail::narrow<size_t>(buffer->size());
      for (auto i = size_t{0}; i < length; ++i) {
        // TODO
        buffer_ptr[i] = invert ? null_ptr[i] : ~null_ptr[i];
      }
    }
    return std::make_shared<arrow::BooleanArray>(l.length(), std::move(buffer));
  }
};

template <ast::binary_op Op, concrete_type R>
  requires((Op == ast::binary_op::eq || Op == ast::binary_op::neq)
           && not std::same_as<R, null_type>)
struct EvalBinOp<Op, null_type, R> {
  static auto eval(const arrow::NullArray& l, const type_to_arrow_array_t<R>& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<Op, R, null_type>::eval(r, l);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: This is bad.
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      auto equal = bool{};
      if (ln != rn) {
        equal = false;
      } else if (ln && rn) {
        equal = true;
      } else {
        equal = l.Value(i) == r.Value(i);
      }
      b.UnsafeAppend(equal != invert);
    }
    return finish(b);
  }
};

} // namespace

auto evaluator::eval(const ast::binary_expr& x) -> series {
  auto eval_op
    = [&]<ast::binary_op Op>(std::integral_constant<ast::binary_op, Op>,
                             const series& l, const series& r) -> series {
    TENZIR_ASSERT(x.op.inner == Op);
    TENZIR_ASSERT(l.length() == r.length());
    return caf::visit(
      [&]<concrete_type L, concrete_type R>(const L&, const R&) -> series {
        if constexpr (caf::detail::is_complete<EvalBinOp<Op, L, R>>) {
          using LA = type_to_arrow_array_t<L>;
          using RA = type_to_arrow_array_t<R>;
          auto& la = caf::get<LA>(*l.array);
          auto& ra = caf::get<RA>(*r.array);
          auto oa = EvalBinOp<Op, L, R>::eval(la, ra);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          // TODO: Not possible?
          // TODO: Where coercion? => coercion is done in kernel.
          diagnostic::warning("binary operator `{}` not implemented for `{}` "
                              "and `{}`",
                              x.op.inner, l.type.kind(), r.type.kind())
            .primary(x)
            .emit(dh_);
          return null();
        }
      },
      l.type, r.type);
  };
  using enum ast::binary_op;
  switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval_op(std::integral_constant<ast::binary_op, op>{}, eval(x.left), \
                   eval(x.right))
    X(add);
    X(sub);
    X(mul);
    X(div);
    X(eq);
    X(neq);
    X(gt);
    X(ge);
    X(lt);
    X(le);
    X(in);
#undef X
    case and_: {
      // TODO: How does short circuiting work?
      // 1) Evaluate left.
      auto l = eval(x.left);
      // 2) Evaluate right, but discard diagnostics if false.
      // TODO
      auto r = eval(x.right);
      return eval_op(std::integral_constant<ast::binary_op, and_>{}, l, r);
    }
    case or_:
      TENZIR_TODO();
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
