//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/active_rows.hpp"
#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/series.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval_impl.hpp"

#include <arrow/compute/api_scalar.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <ranges>

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
    case if_:
    case else_:
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
    case if_:
    case else_:
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
    case if_:
    case else_:
      return false;
  }
  TENZIR_UNREACHABLE();
}

template <ast::binary_op Op, class L, class R>
struct BinOpKernel;

template <>
struct BinOpKernel<ast::binary_op::add, secret_type, secret_type> {
  using result = secret;
  static auto evaluate(secret_view l, secret_view r)
    -> std::variant<result, const char*> {
    return l.with_appended(r);
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, string_type, secret_type> {
  using result = secret;
  static auto evaluate(view3<std::string> l, secret_view r)
    -> std::variant<result, const char*> {
    return r.with_prepended(l);
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, secret_type, string_type> {
  using result = secret;
  static auto evaluate(secret_view l, view3<std::string> r)
    -> std::variant<result, const char*> {
    return l.with_appended(r);
  }
};

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

  static auto evaluate(view3<type_to_data_t<L>> l, view3<type_to_data_t<R>> r)
    -> std::variant<result, const char*> {
    if constexpr (std::same_as<secret_type, L>
                  or std::same_as<secret_type, R>) {
      return "`secret` cannot be compared";
    }
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
  static auto
  eval(const type_to_arrow_array_t<L>& l, const type_to_arrow_array_t<R>& r,
       auto&& warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::Array> {
    using kernel = BinOpKernel<Op, L, R>;
    using result = kernel::result;
    using result_type = data_to_type_t<result>;
    auto b = result_type::make_arrow_builder(arrow_memory_pool());
    auto warnings
      = detail::stack_vector<const char*, 2 * sizeof(const char*)>{};
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i)) {
        check(b->AppendNull());
        continue;
      }
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
        if constexpr (Op == ast::binary_op::eq) {
          check(b->Append(false));
        } else if constexpr (Op == ast::binary_op::neq) {
          check(b->Append(true));
        } else {
          check(b->AppendNull());
        }
        continue;
      }
      auto lv = *view_at<L>(l, i);
      auto rv = *view_at<R>(r, i);
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
                   auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::StringArray> {
    auto b = arrow::StringBuilder{};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i) or l.IsNull(i) or r.IsNull(i)) {
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
struct EvalBinOp<ast::binary_op::in, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r,
                   auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i) or l.IsNull(i) or r.IsNull(i)) {
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
  eval(const ip_type::array_type& l, const subnet_type::array_type& r, auto&&,
       ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i) or l.IsNull(i) or r.IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto ip = *view_at<ip_type>(l, i);
      auto subnet = *view_at<subnet_type>(r, i);
      auto result = subnet.contains(ip);
      check(b.Append(result));
    }
    return finish(b);
  }
};

template <>
struct EvalBinOp<ast::binary_op::in, subnet_type, subnet_type> {
  static auto
  eval(const subnet_type::array_type& l, const subnet_type::array_type& r,
       auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i) or l.IsNull(i) or r.IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto left_subnet = view_at<subnet_type>(l, i);
      auto right_subnet = view_at<subnet_type>(r, i);
      auto result = right_subnet->contains(*left_subnet);
      check(b.Append(result));
    }
    return finish(b);
  }
};

template <ast::binary_op Op, concrete_type L>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r,
                   auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    TENZIR_UNUSED(r);
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i)) {
        check(b.AppendNull());
        continue;
      }
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
                   auto&& warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    return EvalBinOp<Op, R, null_type>::eval(r, l, warn, active);
  }
};

template <ast::binary_op Op>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, ip_type, ip_type> {
  static auto eval(const ip_type::array_type& l, const ip_type::array_type& r,
                   auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: This is bad.
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i)) {
        b.UnsafeAppendNull();
        continue;
      }
      auto ln = l.IsNull(i);
      auto rn = r.IsNull(i);
      auto equal = bool{};
      if (ln != rn) {
        equal = false;
      } else if (ln && rn) {
        equal = true;
      } else {
        equal = *view_at<ip_type>(l, i) == *view_at<ip_type>(r, i);
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
                   auto&&, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: This is bad.
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    for (auto i = int64_t{0}; i < l.length(); ++i) {
      if (not active.is_active(i)) {
        b.UnsafeAppendNull();
        continue;
      }
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

template <basic_type L>
struct EvalBinOp<ast::binary_op::in, L, list_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::ListArray& r,
                   auto&& warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(b.Reserve(l.length()));
    const auto lty = type::from_arrow(*l.type());
    const auto rty = type::from_arrow(*r.value_type());
    const auto f = [&]<concrete_type R>(const R&) {
      if constexpr (basic_type<R>
                    and caf::detail::is_complete<
                      EvalBinOp<ast::binary_op::eq, L, R>>) {
        if constexpr (std::same_as<secret_type, L>
                      or std::same_as<secret_type, R>) {
          warn("`secret` cannot be compared");
          check(b.AppendNulls(l.length()));
        } else {
          // r.values() returns the extension-wrapped child array because Tenzir
          // registers its extension types with Arrow, so MakeArray() wraps them.
          using RA = type_to_arrow_array_t<R>;
          auto* values = dynamic_cast<const RA*>(r.values().get());
          TENZIR_ASSERT(values);
          for (auto i = int64_t{}; i < l.length(); ++i) {
            if (not active.is_active(i)) {
              b.UnsafeAppendNull();
              continue;
            }
            if (r.IsNull(i)) {
              b.UnsafeAppendNull();
              continue;
            }
            auto ln = l.IsNull(i);
            auto list_begin = r.value_offset(i);
            auto list_end = r.value_offset(i + 1);
            auto found = false;
            if (ln) {
              for (auto j = list_begin; j < list_end; ++j) {
                if (values->IsNull(j)) {
                  found = true;
                  break;
                }
              }
            } else {
              auto lv = *view_at<L>(l, i);
              for (auto j = list_begin; j < list_end; ++j) {
                if (values->IsNull(j)) {
                  continue;
                }
                auto rv = *view_at<R>(*values, j);
                if constexpr (integral_type<L> and integral_type<R>) {
                  found = std::cmp_equal(lv, rv);
                } else if constexpr (requires { lv == rv; }) {
                  found = lv == rv;
                }
                if (found) {
                  break;
                }
              }
            }
            b.UnsafeAppend(found);
          }
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

template <ast::binary_op Op, concrete_type L, concrete_type R>
auto eval_op_typed(evaluator& self, ast::binary_expr const& x,
                   basic_series<L> const& left, basic_series<R> const& right,
                   ActiveRows const& active) -> series {
  if constexpr (caf::detail::is_complete<EvalBinOp<Op, L, R>>) {
    auto oa = EvalBinOp<Op, L, R>::eval(
      *left.array, *right.array,
      [&](const char* w) {
        diagnostic::warning("{}", w).primary(x).emit(self.ctx());
      },
      active);
    auto ot = type::from_arrow(*oa->type());
    return series{std::move(ot), std::move(oa)};
  } else {
    diagnostic::warning("binary operator `{}` not implemented for `{}` and "
                        "`{}`",
                        x.op.inner, to_string(type_kind::of<L>),
                        to_string(type_kind::of<R>))
      .primary(x)
      .hint("the result of this expression is `null`")
      .emit(self.ctx());
    return series::null(null_type{}, left.length());
  }
}

template <ast::binary_op Op, concrete_type L>
auto dispatch_eval_rhs(evaluator& self, ast::binary_expr const& x,
                       basic_series<L> const& left, series const& right,
                       ActiveRows const& active) -> series {
  return match(right, [&](auto const& typed) {
    return eval_op_typed<Op>(self, x, left, typed, active);
  });
}

template <ast::binary_op Op>
auto dispatch_eval_binary(evaluator& self, ast::binary_expr const& x,
                          series const& left, series const& right,
                          ActiveRows const& active) -> series {
  return match(left, [&](auto const& typed_left) {
    return dispatch_eval_rhs<Op>(self, x, typed_left, right, active);
  });
}

template <ast::binary_op Op>
auto eval_op(evaluator& self, ast::binary_expr const& x,
             ActiveRows const& active) -> multi_series {
  TENZIR_ASSERT_EQ(x.op.inner, Op);
  auto left = self.eval(x.left, active);
  auto right = self.eval(x.right, active);
  TENZIR_ASSERT_EQ(left.length(), right.length());
  auto offset = int64_t{0};
  return map_series(
    std::move(left), std::move(right), [&](series left, series right) {
      auto part_len = left.length();
      auto active_slice = active.slice(offset, part_len);
      offset += part_len;
      return dispatch_eval_binary<Op>(self, x, left, right, active_slice);
    });
}

template <ast::binary_op Op>
auto eval_and_or(evaluator& self, ast::binary_expr const& x,
                 ActiveRows const& active) -> series {
  // Evaluate the left side and materialize it into a flat BooleanArray.
  // Non-bool left parts are treated as null.
  auto left_ms = self.eval(x.left, active);
  auto left_flat = std::shared_ptr<arrow::BooleanArray>{};
  if (left_ms.parts().size() == 1) {
    auto& left = left_ms.parts().front();
    if (auto typed_left = left.as<bool_type>();
        typed_left and left.length() == self.length()) {
      left_flat = typed_left->array;
    }
  }
  if (not left_flat) {
    auto left_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(left_builder.Reserve(self.length()));
    auto warned_left_mismatch = false;
    for (auto& left : left_ms) {
      auto typed_left = left.as<bool_type>();
      if (not typed_left) {
        if (not is<null_type>(left.type) and not warned_left_mismatch) {
          warned_left_mismatch = true;
          diagnostic::warning("expected `bool`, but got `{}`", left.type.kind())
            .primary(x.left)
            .hint("the result of this expression is `null`")
            .emit(self.ctx());
        }
        check(left_builder.AppendNulls(left.length()));
        continue;
      }
      check(left_builder.AppendArraySlice(*typed_left->array->data(), 0,
                                          left.length()));
    }
    left_flat = finish(left_builder);
  }
  TENZIR_ASSERT_EQ(left_flat->length(), self.length());
  // Evaluate the right side, skipping rows where the left is already
  // deterministic:
  //   and: skip rows where left is definitely false  (skip_value = false)
  //   or:  skip rows where left is definitely true   (skip_value = true)
  // Null entries in left_flat are always active because the result may still
  // depend on the right operand (e.g. null and false == false).
  auto skip_val = (Op == ast::binary_op::or_);
  auto right_active_builder
    = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(right_active_builder.Reserve(self.length()));
  for (auto i = int64_t{0}; i < self.length(); ++i) {
    right_active_builder.UnsafeAppend(
      active.is_active(i)
      and (left_flat->IsNull(i) or left_flat->GetView(i) != skip_val));
  }
  auto right_active = ActiveRows{finish(right_active_builder), false};
  auto right_ms = self.eval_narrowed(x.right, right_active);
  // Combine left_flat and right_ms using three-valued logic.
  auto result_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(result_builder.Reserve(self.length()));
  auto right_offset = int64_t{0};
  for (auto& right : right_ms) {
    auto length = right.length();
    auto right_begin = right_offset;
    right_offset += length;
    auto* right_array = try_as<arrow::BooleanArray>(right.array.get());
    auto warned_right_mismatch = false;
    auto append_from_right = [&](int64_t local_i) {
      if (right_array) {
        if (right_array->IsValid(local_i)) {
          check(result_builder.Append(right_array->GetView(local_i)));
        } else {
          check(result_builder.AppendNull());
        }
        return;
      }
      if (not warned_right_mismatch and not is<null_type>(right.type)) {
        warned_right_mismatch = true;
        diagnostic::warning("expected `bool`, but got `{}`", right.type.kind())
          .primary(x.right)
          .hint("the result of this expression is `null`")
          .emit(self.ctx());
      }
      check(result_builder.AppendNull());
    };
    for (auto local_i = int64_t{0}; local_i < length; ++local_i) {
      auto global_i = right_begin + local_i;
      auto left_valid = left_flat->IsValid(global_i);
      auto left_true = left_valid and left_flat->GetView(global_i);
      if constexpr (Op == ast::binary_op::and_) {
        if (left_true) {
          append_from_right(local_i);
        } else if (left_valid) {
          // left is false: result is false, right was not evaluated
          check(result_builder.Append(false));
        } else {
          // left is null: result is false if right is definitely false, else null
          if (right_array and right_array->IsValid(local_i)
              and not right_array->GetView(local_i)) {
            check(result_builder.Append(false));
          } else {
            check(result_builder.AppendNull());
          }
        }
      } else {
        static_assert(Op == ast::binary_op::or_);
        if (left_true) {
          // left is true: result is true, right was not evaluated
          check(result_builder.Append(true));
        } else if (left_valid) {
          // left is false: result depends on right
          append_from_right(local_i);
        } else {
          // left is null: result is true if right is definitely true, else null
          if (right_array and right_array->IsValid(local_i)
              and right_array->GetView(local_i)) {
            check(result_builder.Append(true));
          } else {
            check(result_builder.AppendNull());
          }
        }
      }
    }
  }
  auto result = finish(result_builder);
  TENZIR_ASSERT_EQ(result->length(), self.length());
  return series{bool_type{}, std::move(result)};
}

auto eval_if(evaluator& self, ast::binary_expr const& x,
             ast::expression const& fallback, ActiveRows const& active)
  -> multi_series;

auto eval_if(evaluator& self, ast::binary_expr const& x,
             ActiveRows const& active) -> multi_series {
  return eval_if(self, x, ast::constant{caf::none, location::unknown}, active);
}

auto eval_if(evaluator& self, ast::binary_expr const& x,
             ast::expression const& fallback, ActiveRows const& active)
  -> multi_series {
  // Materialize condition into a no-null BooleanArray (null → false),
  // evaluate branches with ActiveRows to skip irrelevant rows, then combine.
  auto cond_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(cond_builder.Reserve(self.length()));
  auto warned_cond_type = false;
  auto warned_cond_null = false;
  auto cond_offset = int64_t{0};
  for (auto& cond_part : self.eval(x.right, active)) {
    auto typed = cond_part.as<bool_type>();
    if (not typed) {
      if (not warned_cond_type) {
        auto active_slice = active.slice(cond_offset, cond_part.length());
        auto warn = active_slice.as_constant() == true;
        if (not warn) {
          for (auto i = int64_t{0}; i < cond_part.length(); ++i) {
            if (active_slice.is_active(i)) {
              warn = true;
              break;
            }
          }
        }
        if (warn) {
          warned_cond_type = true;
          diagnostic::warning("expected `bool`, but got `{}`",
                              cond_part.type.kind())
            .primary(x.right)
            .hint("this will be treated as `false`")
            .emit(self.ctx());
        }
      }
      for (auto i = int64_t{0}; i < cond_part.length(); ++i) {
        cond_builder.UnsafeAppend(false);
      }
      cond_offset += cond_part.length();
      continue;
    }
    if (typed->array->null_count() > 0 and not warned_cond_null) {
      auto active_slice = active.slice(cond_offset, cond_part.length());
      auto warn = active_slice.as_constant() == true;
      if (not warn) {
        for (auto i = int64_t{0}; i < cond_part.length(); ++i) {
          if (active_slice.is_active(i) and typed->array->IsNull(i)) {
            warn = true;
            break;
          }
        }
      }
      if (warn) {
        warned_cond_null = true;
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(x.right)
          .hint("use `else` to provide a fallback value")
          .emit(self.ctx());
      }
    }
    for (auto i = int64_t{0}; i < typed->array->length(); ++i) {
      cond_builder.UnsafeAppend(typed->array->IsValid(i)
                                and typed->array->GetView(i));
    }
    cond_offset += cond_part.length();
  }
  auto cond_flat = finish(cond_builder);
  TENZIR_ASSERT_EQ(cond_flat->length(), self.length());
  // Then-branch: active where cond is true (skip_value=false → skip false rows).
  // Else-branch: active where cond is false (skip_value=true → skip true rows).
  auto then_active = ActiveRows{};
  auto else_active = ActiveRows{};
  if (active.as_constant() == true) {
    then_active = ActiveRows{cond_flat, false};
    else_active = ActiveRows{cond_flat, true};
  } else {
    auto then_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    auto else_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    check(then_builder.Reserve(self.length()));
    check(else_builder.Reserve(self.length()));
    for (auto i = int64_t{0}; i < self.length(); ++i) {
      auto cond = cond_flat->GetView(i);
      auto active_row = active.is_active(i);
      then_builder.UnsafeAppend(active_row and cond);
      else_builder.UnsafeAppend(active_row and not cond);
    }
    then_active = ActiveRows{finish(then_builder), false};
    else_active = ActiveRows{finish(else_builder), false};
  }
  auto then_ms = self.eval_narrowed(x.left, then_active);
  auto else_ms = self.eval_narrowed(fallback, else_active);
  // Combine: pick then where cond is true, else where cond is false.
  auto result = multi_series{};
  auto offset = int64_t{0};
  for (auto [then_part, else_part] : split_multi_series(then_ms, else_ms)) {
    auto length = then_part.length();
    auto begin = offset;
    offset += length;
    auto get_cond = [&](int64_t i) -> bool {
      return cond_flat->GetView(begin + i);
    };
    if (length == 0) {
      continue;
    }
    auto range_start = int64_t{0};
    auto range_val = get_cond(0);
    auto append_range = [&](int64_t end) {
      result.append(
        (range_val ? then_part : else_part).slice(range_start, end));
    };
    for (auto i = int64_t{1}; i < length; ++i) {
      if (get_cond(i) == range_val) {
        continue;
      }
      append_range(i);
      range_start = i;
      range_val = not range_val;
    }
    append_range(length);
  }
  TENZIR_ASSERT_EQ(result.length(), self.length());
  return result;
}

auto eval_else(evaluator& self, ast::binary_expr const& x,
               ActiveRows const& active) -> multi_series {
  // Short-circuit the evaluation of `x if y else z`, avoiding the
  // construction of null series. This is also important for correctness, as
  // `null if true else 42` should return `null`, but without this would return
  // `42`.
  if (auto const* binop = try_as<ast::binary_expr>(x.left)) {
    if (binop->op.inner == ast::binary_op::if_) {
      return eval_if(self, *binop, x.right, active);
    }
  }
  // Evaluate left, build a null mask, then evaluate right only for
  // rows where left is null, and combine.
  auto left_ms = self.eval(x.left, active);
  // Build the final active mask for the right-hand side directly.
  auto right_active_builder
    = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(right_active_builder.Reserve(self.length()));
  auto offset = int64_t{0};
  for (auto& left_part : left_ms) {
    for (auto i = int64_t{0}; i < left_part.length(); ++i) {
      right_active_builder.UnsafeAppend(active.is_active(offset + i)
                                        and left_part.array->IsNull(i));
    }
    offset += left_part.length();
  }
  auto right_active = ActiveRows{finish(right_active_builder), false};
  // Evaluate right only where left is null on rows that are still active.
  auto right_ms = self.eval_narrowed(x.right, right_active);
  // Combine: pick left where non-null, else pick right.
  auto result = multi_series{};
  for (auto [left_part, right_part] : split_multi_series(left_ms, right_ms)) {
    auto length = left_part.length();
    if (left_part.array->null_count() == 0) {
      result.append(left_part);
      continue;
    }
    if (left_part.array->null_count() == length) {
      result.append(right_part);
      continue;
    }
    auto range_start = int64_t{0};
    auto range_valid = left_part.array->IsValid(0);
    auto append_range = [&](int64_t end) {
      if (range_valid) {
        result.append(left_part.slice(range_start, end));
      } else {
        result.append(right_part.slice(range_start, end));
      }
    };
    for (auto i = int64_t{1}; i < length; ++i) {
      if (left_part.array->IsValid(i) == range_valid) {
        continue;
      }
      append_range(i);
      range_start = i;
      range_valid = not range_valid;
    }
    append_range(length);
  }
  TENZIR_ASSERT_EQ(result.length(), self.length());
  return result;
}

} // namespace

auto evaluator::eval(ast::binary_expr const& x, ActiveRows const& active)
  -> multi_series {
  switch (x.op.inner) {
#define X(op)                                                                  \
  case ast::binary_op::op:                                                     \
    return eval_op<ast::binary_op::op>(*this, x, active)
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
#undef X
      // These four have special handling as they short-circuit the evaluation
      // of either side of the expression.
    case ast::binary_op::and_:
      return eval_and_or<ast::binary_op::and_>(*this, x, active);
    case ast::binary_op::or_:
      return eval_and_or<ast::binary_op::or_>(*this, x, active);
    case ast::binary_op::if_:
      return eval_if(*this, x, active);
    case ast::binary_op::else_:
      return eval_else(*this, x, active);
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
