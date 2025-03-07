//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/series.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval_impl.hpp"

// TODO: This file takes very long to compile. Consider splitting it up even more.

namespace tenzir {

namespace {

[[maybe_unused]] constexpr auto is_arithmetic(ast::binary_op op) -> bool {
  using enum ast::binary_op;
  switch (op) {
    case add:
    case sub:
    case mul:
    case div:
      return true;
    case eq:
    case neq:
    case gt:
    case geq:
    case lt:
    case leq:
    case and_:
    case or_:
    case in:
      return false;
  }
  TENZIR_UNREACHABLE();
}

[[maybe_unused]] constexpr auto result_if_both_null(ast::binary_op op)
  -> std::optional<bool> {
  using enum ast::binary_op;
  switch (op) {
    case eq:
    case geq:
    case leq:
      return true;
    case neq:
      return false;
    case add:
    case sub:
    case mul:
    case div:
    case gt:
    case lt:
    case and_:
    case or_:
    case in:
      return std::nullopt;
  }
  TENZIR_UNREACHABLE();
}

[[maybe_unused]] constexpr auto is_relational(ast::binary_op op) -> bool {
  using enum ast::binary_op;
  switch (op) {
    case eq:
    case neq:
    case gt:
    case lt:
    case geq:
    case leq:
      return true;
    case add:
    case sub:
    case mul:
    case div:
    case and_:
    case or_:
    case in:
      return false;
  }
  TENZIR_UNREACHABLE();
}

template <ast::binary_op Op, class L, class R>
struct BinOpKernel;

template <ast::binary_op Op, integral_type L, integral_type R>
  requires(is_arithmetic(Op))
struct BinOpKernel<Op, L, R> {
  // TODO: This is just so that we can have the return type be inferred. This
  // would not be necessary if the template itself would be friendly to type
  // inference.
  static auto inner(type_to_data_t<L> l, type_to_data_t<R> r) {
    using enum ast::binary_op;
    if constexpr (Op == add) {
      return checked_add(l, r);
    } else if constexpr (Op == sub) {
      return checked_sub(l, r);
    } else if constexpr (Op == mul) {
      return checked_mul(l, r);
    } else {
      static_assert(detail::always_false_v<L>,
                    "division is handled by its own specialization");
    }
  }

  using result = decltype(inner(std::declval<type_to_data_t<L>>(),
                                std::declval<type_to_data_t<R>>()))::value_type;

  static auto evaluate(type_to_data_t<L> l, type_to_data_t<R> r)
    -> std::variant<result, const char*> {
    auto result = inner(l, r);
    if (not result) {
      return "integer overflow";
    }
    return *result;
  }
};

template <ast::binary_op Op, numeric_type L, numeric_type R>
  requires(is_arithmetic(Op)
           and (std::same_as<double_type, L> or std::same_as<double_type, R>))
struct BinOpKernel<Op, L, R> {
  using result = double;

  static auto evaluate(type_to_data_t<L> l, type_to_data_t<R> r)
    -> std::variant<result, const char*> {
    using enum ast::binary_op;
    if constexpr (Op == add) {
      return static_cast<double>(l) + static_cast<double>(r);
    }
    if constexpr (Op == sub) {
      return static_cast<double>(l) - static_cast<double>(r);
    }
    if constexpr (Op == mul) {
      return static_cast<double>(l) * static_cast<double>(r);
    }
    TENZIR_UNREACHABLE();
  }
};

template <numeric_type L, numeric_type R>
struct BinOpKernel<ast::binary_op::div, L, R> {
  using result = double;

  static auto evaluate(type_to_data_t<L> l, type_to_data_t<R> r)
    -> std::variant<result, const char*> {
    if (r == decltype(r){}) {
      return "division by zero";
    }
    return static_cast<double>(l) / static_cast<double>(r);
  }
};

template <>
struct BinOpKernel<ast::binary_op::sub, time_type, duration_type> {
  using result = time;

  static auto evaluate(time l, duration r)
    -> std::variant<result, const char*> {
    return l - r;
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, time_type, duration_type> {
  using result = time;

  static auto evaluate(time l, duration r)
    -> std::variant<result, const char*> {
    return l + r;
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, duration_type, time_type> {
  using result = time;

  static auto evaluate(duration l, time r)
    -> std::variant<result, const char*> {
    return l + r;
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, duration_type, duration_type> {
  using result = duration;

  static auto evaluate(duration l, duration r)
    -> std::variant<result, const char*> {
    if (auto check = checked_add(l.count(), r.count())) {
      return duration{check.value()};
    }
    return "duration addition overflow";
  }
};

template <>
struct BinOpKernel<ast::binary_op::sub, duration_type, duration_type> {
  using result = duration;

  static auto evaluate(duration l, duration r)
    -> std::variant<result, const char*> {
    if (auto check = checked_sub(l.count(), r.count())) {
      return duration{check.value()};
    }
    return "duration subtraction overflow";
  }
};

template <>
struct BinOpKernel<ast::binary_op::div, duration_type, duration_type> {
  using result = double;

  static auto evaluate(duration l, duration r)
    -> std::variant<result, const char*> {
    if (r == decltype(r){}) {
      return "division by zero";
    }
    return detail::narrow_cast<result>(l.count())
           / detail::narrow_cast<result>(r.count());
  }
};

template <integral_type N>
struct BinOpKernel<ast::binary_op::mul, duration_type, N> {
  using result = duration;

  static auto evaluate(duration l, type_to_data_t<N> r)
    -> std::variant<result, const char*> {
    if (auto check = checked_mul(l.count(), r); check.has_value()) {
      return duration{check.value()};
    }
    return "duration multiplication overflow";
  }
};

template <>
struct BinOpKernel<ast::binary_op::mul, duration_type, double_type> {
  using result = duration;

  static auto evaluate(duration l, double r)
    -> std::variant<result, const char*> {
    return duration_cast<duration>(l * r);
  }
};

template <numeric_type N>
struct BinOpKernel<ast::binary_op::mul, N, duration_type> {
  using result = duration;

  static auto evaluate(type_to_data_t<N> l, duration r)
    -> std::variant<result, const char*> {
    return BinOpKernel<ast::binary_op::mul, duration_type, N>::evaluate(
      r, l); // Commutative
  }
};

template <numeric_type N>
struct BinOpKernel<ast::binary_op::div, duration_type, N> {
  using result = duration;

  static auto evaluate(duration l, type_to_data_t<N> r)
    -> std::variant<result, const char*> {
    if (r == decltype(r){}) {
      return "division by zero";
    }
    return std::chrono::duration_cast<duration>(l / r);
  }
};

template <>
struct BinOpKernel<ast::binary_op::sub, time_type, time_type> {
  using result = duration;

  static auto evaluate(time l, time r) -> std::variant<result, const char*> {
    return l - r;
  }
};

template <ast::binary_op Op, basic_type L, basic_type R>
  requires(is_relational(Op) and not(integral_type<L> and integral_type<R>)
           and requires(type_to_data_t<L> l, type_to_data_t<R> r) { l <=> r; })
struct BinOpKernel<Op, L, R> {
  using result = bool;

  static auto evaluate(view<type_to_data_t<L>> l, view<type_to_data_t<R>> r)
    -> std::variant<result, const char*> {
    using enum ast::binary_op;
    if constexpr (Op == eq) {
      return l == r;
    } else if constexpr (Op == neq) {
      return l != r;
    } else if constexpr (Op == gt) {
      return l > r;
    } else if constexpr (Op == lt) {
      return l < r;
    } else if constexpr (Op == geq) {
      return l >= r;
    } else if constexpr (Op == leq) {
      return l <= r;
    } else {
      static_assert(detail::always_false_v<L>,
                    "unexpected operator in relational kernel");
    }
  }
};

template <ast::binary_op Op, integral_type L, integral_type R>
  requires(is_relational(Op))
struct BinOpKernel<Op, L, R> {
  using result = bool;

  static auto evaluate(type_to_data_t<L> l, type_to_data_t<R> r)
    -> std::variant<result, const char*> {
    using enum ast::binary_op;
    if constexpr (Op == eq) {
      return std::cmp_equal(l, r);
    } else if constexpr (Op == neq) {
      return std::cmp_not_equal(l, r);
    } else if constexpr (Op == gt) {
      return std::cmp_greater(l, r);
    } else if constexpr (Op == lt) {
      return std::cmp_less(l, r);
    } else if constexpr (Op == geq) {
      return std::cmp_greater_equal(l, r);
    } else if constexpr (Op == leq) {
      return std::cmp_less_equal(l, r);
    } else {
      static_assert(detail::always_false_v<L>,
                    "unexpected operator in relational kernel");
    }
  }
};

template <ast::binary_op Op, concrete_type L, concrete_type R>
struct EvalBinOp;

// specialization for cases where a kernel is implemented
template <ast::binary_op Op, basic_type L, basic_type R>
  requires caf::detail::is_complete<BinOpKernel<Op, L, R>>
struct EvalBinOp<Op, L, R> {
  static auto eval(const type_to_arrow_array_t<L>& l,
                   const type_to_arrow_array_t<R>& r, auto&& warn)
    -> std::shared_ptr<arrow::Array> {
    using kernel = BinOpKernel<Op, L, R>;
    using result = kernel::result;
    using result_type = data_to_type_t<result>;
    auto b = result_type::make_arrow_builder(arrow::default_memory_pool());
    auto warnings
      = detail::stack_vector<const char*, 2 * sizeof(const char*)>{};
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      if (ln && rn) {
        constexpr auto res = result_if_both_null(Op);
        if constexpr (res) {
          check(b->Append(*res));
        } else {
          check(b->AppendNull());
        }
        continue;
      }
      if (ln || rn) {
        check(b->AppendNull());
        continue;
      }
      auto lv = value_at(L{}, l, i);
      auto rv = value_at(R{}, r, i);
      auto res = kernel::evaluate(lv, rv);
      if (auto r = std::get_if<result>(&res)) {
        check(append_builder(result_type{}, *b, *r));
      } else {
        check(b->AppendNull());
        auto e = std::get<const char*>(res);
        if (std::ranges::find(warnings, e) == std::ranges::end(warnings)) {
          warnings.push_back(e);
        }
      }
    }
    for (auto&& w : warnings) {
      warn(w);
    }
    return finish(*b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::add, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r,
                   auto&&) -> std::shared_ptr<arrow::StringArray> {
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

template <>
struct EvalBinOp<ast::binary_op::and_, bool_type, null_type> {
  static auto eval(const arrow::BooleanArray& l, const arrow::NullArray&,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (const auto& v : l) {
      if (v and v == false) {
        check(b.Append(false));
      } else {
        check(b.AppendNull());
      }
    }
    return finish(b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::and_, null_type, bool_type> {
  static auto eval(const arrow::NullArray& l, const arrow::BooleanArray& r,
                   auto&& warn) -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<ast::binary_op::and_, bool_type, null_type>::eval(r, l,
                                                                       warn);
  }
};

template <>
struct EvalBinOp<ast::binary_op::or_, bool_type, null_type> {
  static auto eval(const arrow::BooleanArray& l, const arrow::NullArray&,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (const auto& v : l) {
      if (v and v == true) {
        check(b.Append(true));
      } else {
        check(b.AppendNull());
      }
    }
    return finish(b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::or_, null_type, bool_type> {
  static auto eval(const arrow::NullArray& l, const arrow::BooleanArray& r,
                   auto&& warn) -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<ast::binary_op::or_, bool_type, null_type>::eval(r, l,
                                                                      warn);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::and_ || Op == ast::binary_op::or_)
struct EvalBinOp<Op, null_type, null_type> {
  static auto eval(const arrow::NullArray& l, const arrow::NullArray&, auto&&)
    -> std::shared_ptr<arrow::NullArray> {
    return std::make_shared<arrow::NullArray>(l.length());
  }
};

template <>
struct EvalBinOp<ast::binary_op::in, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        b.UnsafeAppendNull();
        continue;
      }
      auto lv = l.GetView(i);
      auto rv = r.GetView(i);
      check(b.Append(rv.find(lv) != rv.npos));
    }
    return finish(b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::in, ip_type, subnet_type> {
  static auto
  eval(const ip_type::array_type& l, const subnet_type::array_type& r, auto&&)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto ip = value_at(ip_type{}, l, i);
      auto subnet = value_at(subnet_type{}, r, i);
      auto result = subnet.contains(ip);
      check(b.Append(result));
    }
    return finish(b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::in, subnet_type, subnet_type> {
  static auto eval(const subnet_type::array_type& l,
                   const subnet_type::array_type& r, auto&&)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (l.IsNull(i) || r.IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto left_subnet = value_at(subnet_type{}, l, i);
      auto right_subnet = value_at(subnet_type{}, r, i);
      auto result = right_subnet.contains(left_subnet);
      check(b.Append(result));
    }
    return finish(b);
  }
};

// TODO: We probably don't want this.
template <ast::binary_op Op>
  requires(Op == ast::binary_op::and_ || Op == ast::binary_op::or_)
struct EvalBinOp<Op, bool_type, bool_type> {
  static constexpr auto is_and = Op == ast::binary_op::and_;

  static auto eval(const arrow::BooleanArray& l, const arrow::BooleanArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: Bad.
    auto has_null = l.null_bitmap() || r.null_bitmap();
    auto has_offset = l.offset() != 0 || r.offset() != 0;
    if (not has_null && not has_offset) {
      auto buffer = check(arrow::AllocateBitmap(l.length()));
      auto size = (l.length() + 7) / 8;
      TENZIR_ASSERT(l.values()->size() >= size);
      TENZIR_ASSERT(r.values()->size() >= size);
      const auto* l_ptr = l.values()->data();
      const auto* r_ptr = r.values()->data();
      auto* o_ptr = buffer->mutable_data();
      for (auto i = int64_t{0}; i < size; ++i) {
        if constexpr (is_and) {
          o_ptr[i] = l_ptr[i] & r_ptr[i]; // NOLINT
        } else {
          o_ptr[i] = l_ptr[i] | r_ptr[i]; // NOLINT
        }
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
      if constexpr (is_and) {
        if (lt && rt) {
          b.UnsafeAppend(true);
        } else if (lf || rf) {
          b.UnsafeAppend(false);
        } else {
          b.UnsafeAppendNull();
        }
      } else {
        if (lt || rt) {
          b.UnsafeAppend(true);
        } else if (lf && rf) {
          b.UnsafeAppend(false);
        } else {
          b.UnsafeAppendNull();
        }
      }
    }
    return finish(b);
  }
};

template <ast::binary_op Op, concrete_type L>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    TENZIR_UNUSED(r);
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{};
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      check(b.Append(l.IsNull(i) != invert));
    }
    return finish(b);
  }
};

template <ast::binary_op Op, concrete_type R>
  requires((Op == ast::binary_op::eq || Op == ast::binary_op::neq)
           && not std::same_as<R, null_type>)
struct EvalBinOp<Op, null_type, R> {
  static auto eval(const arrow::NullArray& l, const type_to_arrow_array_t<R>& r,
                   auto&& warn) -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<Op, R, null_type>::eval(r, l, warn);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, ip_type, ip_type> {
  static auto eval(const ip_type::array_type& l, const ip_type::array_type& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
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
        equal = value_at(ip_type{}, l, i) == value_at(ip_type{}, r, i);
      }
      b.UnsafeAppend(equal != invert);
    }
    return finish(b);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
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
        equal = l.GetView(i) == r.GetView(i);
      }
      b.UnsafeAppend(equal != invert);
    }
    return finish(b);
  }
};

template <concrete_type L>
struct EvalBinOp<ast::binary_op::in, L, list_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::ListArray& r,
                   auto&& warn) -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{};
    check(b.Reserve(l.length()));
    const auto lty = type::from_arrow(*l.type());
    const auto rty = type::from_arrow(*r.value_type());
    const auto f = [&]<concrete_type R>(const R&) {
      if constexpr (caf::detail::is_complete<
                      EvalBinOp<ast::binary_op::eq, L, R>>) {
        for (auto i = int64_t{}; i < l.length(); ++i) {
          if (r.IsNull(i)) {
            b.UnsafeAppendNull();
            continue;
          }
          auto lslice
            = std::static_pointer_cast<type_to_arrow_array_t<L>>(l.Slice(i, 1));
          auto rslice = r.value_slice(i);
          TENZIR_ASSERT(lslice);
          TENZIR_ASSERT(rslice);
          auto result = false;
          for (auto j = int64_t{}; j < rslice->length(); ++j) {
            auto vals = std::dynamic_pointer_cast<type_to_arrow_array_t<R>>(
              rslice->Slice(j, 1));
            TENZIR_ASSERT(vals);
            auto out = std::dynamic_pointer_cast<arrow::BooleanArray>(
              EvalBinOp<ast::binary_op::eq, L, R>::eval(*lslice, *vals, warn));
            TENZIR_ASSERT(out);
            TENZIR_ASSERT(out->length() == 1);
            // Equality never returns `null` (if it's defined for the types).
            TENZIR_ASSERT(out->IsValid(0));
            if (out->Value(0)) {
              result = true;
              break;
            }
          }
          check(b.Append(result));
        }
      } else {
        warn(fmt::format("got incompatible types for `in`: `{} in list<{}>`",
                         lty.kind(), rty.kind())
               .c_str());
        check(b.AppendNulls(l.length()));
      }
    };
    match(rty, f);
    return finish(b);
  }
};

} // namespace

auto evaluator::eval(const ast::binary_expr& x) -> multi_series {
  auto eval_op
    = [&]<ast::binary_op Op>(multi_series l, multi_series r) -> multi_series {
    TENZIR_ASSERT(x.op.inner == Op);
    TENZIR_ASSERT(l.length() == r.length());
    return map_series(std::move(l), std::move(r), [&](series l, series r) {
      return match(
        std::tie(l.type, r.type),
        [&]<concrete_type L, concrete_type R>(const L&, const R&) -> series {
          if constexpr (caf::detail::is_complete<EvalBinOp<Op, L, R>>) {
            using LA = type_to_arrow_array_t<L>;
            using RA = type_to_arrow_array_t<R>;
            auto& la = as<LA>(*l.array);
            auto& ra = as<RA>(*r.array);
            auto oa = EvalBinOp<Op, L, R>::eval(la, ra, [&](const char* w) {
              diagnostic::warning("{}", w).primary(x).emit(ctx_);
            });
            auto ot = type::from_arrow(*oa->type());
            return series{std::move(ot), std::move(oa)};
          } else {
            // TODO: Not possible?
            // TODO: Where coercion? => coercion is done in kernel.
            diagnostic::warning("binary operator `{}` not implemented for `{}` "
                                "and `{}`",
                                x.op.inner, l.type.kind(), r.type.kind())
              .primary(x)
              .emit(ctx_);
            return null();
          }
        });
    });
  };
  using enum ast::binary_op;
  switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval_op.operator()<op>(eval(x.left), eval(x.right))
    X(add);
    X(sub);
    X(mul);
    X(div);
    X(eq);
    X(neq);
    X(gt);
    X(geq);
    X(lt);
    X(leq);
    X(in);
    // TODO: Short circuiting.
    X(and_);
    X(or_);
#undef X
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
