//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
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

auto disable_short_circuiting() -> bool {
  static const auto result = [] {
    auto value = detail::getenv("TENZIR_EVAL_DISABLE_SHORT_CIRCUITING");
    if (not value) {
      return false;
    }
    auto lower = *value;
    std::ranges::transform(lower, lower.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return lower != "0" and lower != "false" and lower != "no"
           and lower != "off";
  }();
  return result;
}

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
  static auto evaluate(view<std::string> l, secret_view r)
    -> std::variant<result, const char*> {
    return r.with_prepended(l);
  }
};

template <>
struct BinOpKernel<ast::binary_op::add, secret_type, string_type> {
  using result = secret;
  static auto evaluate(secret_view l, view<std::string> r)
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

  static auto evaluate(view<type_to_data_t<L>> l, view<type_to_data_t<R>> r)
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
  static auto eval(const type_to_arrow_array_t<L>& l,
                   const type_to_arrow_array_t<R>& r, auto&& warn)
    -> std::shared_ptr<arrow::Array> {
    using kernel = BinOpKernel<Op, L, R>;
    using result = kernel::result;
    using result_type = data_to_type_t<result>;
    auto b = result_type::make_arrow_builder(arrow_memory_pool());
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
        if constexpr (Op == ast::binary_op::eq) {
          check(b->Append(false));
        } else if constexpr (Op == ast::binary_op::neq) {
          check(b->Append(true));
        } else {
          check(b->AppendNull());
        }
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
struct EvalBinOp<ast::binary_op::in, string_type, string_type> {
  static auto eval(const arrow::StringArray& l, const arrow::StringArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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

template <ast::binary_op Op, concrete_type L>
  requires(Op == ast::binary_op::eq || Op == ast::binary_op::neq)
struct EvalBinOp<Op, L, null_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::NullArray& r,
                   auto&&) -> std::shared_ptr<arrow::BooleanArray> {
    TENZIR_UNUSED(r);
    constexpr auto invert = Op == ast::binary_op::neq;
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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
    auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
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

template <basic_type L>
struct EvalBinOp<ast::binary_op::in, L, list_type> {
  static auto eval(const type_to_arrow_array_t<L>& l, const arrow::ListArray& r,
                   auto&& warn) -> std::shared_ptr<arrow::BooleanArray> {
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
              auto lv = value_at(L{}, l, i);
              for (auto j = list_begin; j < list_end; ++j) {
                if (values->IsNull(j)) {
                  continue;
                }
                auto rv = value_at(R{}, *values, j);
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

template <ast::binary_op Op>
auto eval_op(evaluator& self, const ast::binary_expr& x) -> multi_series {
  TENZIR_ASSERT_EQ(x.op.inner, Op);
  auto left = self.eval(x.left);
  auto right = self.eval(x.right);
  TENZIR_ASSERT_EQ(left.length(), right.length());
  return map_series(
    std::move(left), std::move(right), [&](series left, series right) {
      return match(
        std::tie(left.type, right.type),
        [&]<concrete_type L, concrete_type R>(const L&, const R&) -> series {
          if constexpr (caf::detail::is_complete<EvalBinOp<Op, L, R>>) {
            using LA = type_to_arrow_array_t<L>;
            using RA = type_to_arrow_array_t<R>;
            auto& la = as<LA>(*left.array);
            auto& ra = as<RA>(*right.array);
            auto oa = EvalBinOp<Op, L, R>::eval(la, ra, [&](const char* w) {
              diagnostic::warning("{}", w).primary(x).emit(self.ctx());
            });
            auto ot = type::from_arrow(*oa->type());
            return series{std::move(ot), std::move(oa)};
          } else {
            // TODO: Not possible?
            // TODO: Where coercion? => coercion is done in kernel.
            diagnostic::warning("binary operator `{}` not implemented for `{}` "
                                "and `{}`",
                                x.op.inner, left.type.kind(), right.type.kind())
              .primary(x)
              .hint("the result of this expression is `null`")
              .emit(self.ctx());
            return series::null(null_type{}, left.length());
          }
        });
    });
}

template <ast::binary_op Op>
  requires(Op == ast::binary_op::and_ or Op == ast::binary_op::or_)
auto eval_and_or_eager(evaluator& self, const ast::binary_expr& x) -> series {
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(self.length()));
  for (auto [left, right] :
       split_multi_series(self.eval(x.left), self.eval(x.right))) {
    const auto length = left.length();
    TENZIR_ASSERT_EQ(length, right.length());
    const auto typed_left = left.as<bool_type>();
    if (not typed_left) {
      diagnostic::warning("expected `bool`, but got `{}`", left.type.kind())
        .primary(x.left)
        .hint("the result of this expression is `null`")
        .emit(self.ctx());
      const auto typed_right = right.as<bool_type>();
      if (not typed_right) {
        diagnostic::warning("expected `bool`, but got `{}`", right.type.kind())
          .primary(x.right)
          .hint("the result of this expression is `null`")
          .emit(self.ctx());
      }
      for (auto i = int64_t{0}; i < length; ++i) {
        if constexpr (Op == ast::binary_op::or_) {
          if (typed_right and typed_right->array->IsValid(i)
              and typed_right->array->GetView(i)) {
            check(builder.Append(true));
          } else {
            check(builder.AppendNull());
          }
        } else {
          if (typed_right and typed_right->array->IsValid(i)
              and not typed_right->array->GetView(i)) {
            check(builder.Append(false));
          } else {
            check(builder.AppendNull());
          }
        }
      }
      continue;
    }
    auto* const right_array = try_as<arrow::BooleanArray>(right.array.get());
    auto warned_right_mismatch = false;
    const auto append_from_right = [&](int64_t i) {
      if (right_array) {
        if (right_array->IsValid(i)) {
          check(builder.Append(right_array->GetView(i)));
        } else {
          check(builder.AppendNull());
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
      check(builder.AppendNull());
    };
    for (auto i = int64_t{0}; i < length; ++i) {
      const auto left_valid = typed_left->array->IsValid(i);
      const auto left_true = left_valid and typed_left->array->GetView(i);
      if constexpr (Op == ast::binary_op::and_) {
        if (left_true) {
          append_from_right(i);
        } else if (left_valid) {
          check(builder.Append(false));
        } else {
          check(builder.AppendNull());
        }
      } else {
        static_assert(Op == ast::binary_op::or_);
        if (left_true) {
          check(builder.Append(true));
        } else {
          append_from_right(i);
        }
      }
    }
  }
  auto result = finish(builder);
  TENZIR_ASSERT_EQ(result->length(), self.length());
  return series{
    bool_type{},
    std::move(result),
  };
}

template <ast::binary_op Op>
auto eval_and_or(evaluator& self, const ast::binary_expr& x) -> series {
  if (disable_short_circuiting()) {
    return eval_and_or_eager<Op>(self, x);
  }
  auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  check(builder.Reserve(self.length()));
  auto left_offset = int64_t{0};
  for (const auto& left : self.eval(x.left)) {
    const auto length = left.length();
    const auto left_begin = left_offset;
    const auto left_end = left_begin + length;
    left_offset += length;
    const auto typed_left = left.as<bool_type>();
    if (not typed_left) {
      if (not is<null_type>(left.type)) {
        diagnostic::warning("expected `bool`, but got `{}`", left.type.kind())
          .primary(x.left)
          .hint("the result of this expression is `null`")
          .emit(self.ctx());
      }
    }
    const auto short_circuit_eval_right = [&]<bool Value>() {
      for (const auto& right : self.slice(left_begin, left_end).eval(x.right)) {
        if (const auto typed_right = right.as<bool_type>()) {
          for (const auto& value : *typed_right->array) {
            if (value and *value == Value) {
              check(builder.Append(Value));
              continue;
            }
            check(builder.AppendNull());
          }
          continue;
        }
        if (not is<null_type>(right.type)) {
          diagnostic::warning("expected `bool`, but got `{}`",
                              right.type.kind())
            .primary(x.right)
            .hint("the result of this expression is `null`")
            .emit(self.ctx());
        }
        check(builder.AppendNulls(right.length()));
      }
    };
    if constexpr (Op == ast::binary_op::and_) {
      if (not typed_left) {
        short_circuit_eval_right.template operator()<false>();
        TENZIR_ASSERT_EQ(builder.length(), left_offset);
        continue;
      }
      if (typed_left->array->false_count() == length) {
        check(builder.AppendArraySlice(*typed_left->array->data(), 0, length));
        TENZIR_ASSERT_EQ(builder.length(), left_offset);
        continue;
      }
    } else if constexpr (Op == ast::binary_op::or_) {
      if (not typed_left) {
        short_circuit_eval_right.template operator()<true>();
        TENZIR_ASSERT_EQ(builder.length(), left_offset);
        continue;
      }
      if (typed_left->array->true_count() == length) {
        check(builder.AppendArraySlice(*typed_left->array->data(), 0, length));
        TENZIR_ASSERT_EQ(builder.length(), left_offset);
        continue;
      }
    } else {
      static_assert(detail::always_false_v<decltype(Op)>, "unsupported op");
    }
    TENZIR_ASSERT(typed_left);
    const auto get_left = [&](int64_t i) -> bool {
      return typed_left->array->IsValid(i) and typed_left->array->GetView(i);
    };
    const auto eval_right = [&](int64_t start, int64_t end) -> void {
      TENZIR_ASSERT_LT(start, end);
      for (const auto& right :
           self.slice(left_begin + start, left_begin + end).eval(x.right)) {
        if (is<bool_type>(right.type)) {
          check(
            builder.AppendArraySlice(*right.array->data(), 0, right.length()));
          continue;
        }
        if (not is<null_type>(right.type)) {
          diagnostic::warning("expected `bool`, but got `{}`",
                              right.type.kind())
            .primary(x.right)
            .hint("the result of this expression is `null`")
            .emit(self.ctx());
        }
        check(builder.AppendNulls(right.length()));
      }
    };
    auto range_offset = int64_t{0};
    auto range_current = get_left(0);
    const auto append_until = [&](int64_t end) {
      TENZIR_ASSERT_GT(end, range_offset);
      TENZIR_ASSERT_LT(range_offset, length);
      if constexpr (Op == ast::binary_op::and_ or Op == ast::binary_op::or_) {
        if (range_current == (Op == ast::binary_op::and_)) {
          eval_right(range_offset, end);
        } else {
          check(builder.AppendArraySlice(*left.array->data(), range_offset,
                                         end - range_offset));
        }
      } else {
        static_assert(detail::always_false_v<decltype(Op)>, "unsupported op");
      }
      TENZIR_ASSERT_EQ(builder.length(), left_begin + end);
    };
    for (auto i = int64_t{1}; i < length; ++i) {
      if (range_current == get_left(i)) {
        continue;
      }
      append_until(i);
      range_offset = i;
      range_current = not range_current;
    }
    append_until(length);
    TENZIR_ASSERT_EQ(builder.length(), left_offset);
  }
  auto result = finish(builder);
  TENZIR_ASSERT_EQ(result->length(), self.length());
  return series{
    bool_type{},
    std::move(result),
  };
}

auto eval_if(evaluator& self, const ast::binary_expr& x,
             const ast::expression& fallback
             = ast::constant{caf::none, location::unknown}) -> multi_series {
  if (disable_short_circuiting()) {
    auto inputs = std::array<multi_series, 3>{
      self.eval(x.right),
      self.eval(x.left),
      self.eval(fallback),
    };
    // TODO: There is an optimization possibility here where the `predicate` is
    // iterated independently of the `map_series` call, removing splits based on
    // its type change, given that in the end we only care about
    // `bool(predicate) == true` vs everything else. Notably this only matters
    // for cases where the predicate itself evaluates to a heavily split series,
    // which should be fairly rare, with the most common exception presumably
    // being untyped nulls.
    return map_series(inputs, [&](std::span<series> parts) -> multi_series {
      TENZIR_ASSERT_EQ(parts.size(), 3u);
      const auto& predicate = parts[0];
      const auto& then_branch = parts[1];
      const auto& else_branch = parts[2];
      const auto length = predicate.length();
      TENZIR_ASSERT_EQ(length, then_branch.length());
      TENZIR_ASSERT_EQ(length, else_branch.length());
      const auto typed_predicate = predicate.as<bool_type>();
      if (not typed_predicate) {
        diagnostic::warning("expected `bool`, but got `{}`",
                            predicate.type.kind())
          .primary(x.right)
          .hint("this will be treated as `false`")
          .emit(self.ctx());
        return else_branch;
      }
      if (typed_predicate->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(x.right)
          .hint("use `else` to provide a fallback value")
          .emit(self.ctx());
      }
      if (typed_predicate->array->true_count() == length) {
        return then_branch;
      }
      if (typed_predicate->array->true_count() == 0) {
        return else_branch;
      }
      // Fast path: use Arrow's kernel when both branches have the same type.
      if (then_branch.type == else_branch.type) {
        auto predicate_array
          = std::shared_ptr<arrow::BooleanArray>{typed_predicate->array};
        // `if_else` promotes nulls in the predicate to null outputs. We treat
        // null predicate values as false and pick the fallback branch instead.
        if (typed_predicate->array->null_count() > 0) {
          auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
          check(b.Reserve(length));
          for (auto i = int64_t{0}; i < length; ++i) {
            b.UnsafeAppend(typed_predicate->array->IsValid(i)
                           and typed_predicate->array->GetView(i));
          }
          predicate_array = finish(b);
        }
        auto if_else_result = arrow::compute::IfElse(
          predicate_array, then_branch.array, else_branch.array);
        if (if_else_result.ok()) {
          TENZIR_ASSERT(if_else_result->is_array());
          return series{then_branch.type, if_else_result->make_array()};
        }
      }
      const auto get_predicate = [&](int64_t i) -> bool {
        return typed_predicate->array->IsValid(i)
               and typed_predicate->array->GetView(i);
      };
      auto result = multi_series{};
      auto range_offset = int64_t{0};
      auto range_current = get_predicate(0);
      const auto append_until = [&](int64_t end) {
        result.append(
          (range_current ? then_branch : else_branch).slice(range_offset, end));
      };
      for (auto i = int64_t{1}; i < length; ++i) {
        if (range_current == get_predicate(i)) {
          continue;
        }
        append_until(i);
        range_offset = i;
        range_current = not range_current;
      }
      append_until(length);
      TENZIR_ASSERT_EQ(result.length(), length);
      return result;
    });
  }
  auto right_offset = int64_t{0};
  return map_series(
    self.eval(x.right), [&](const series& right) -> multi_series {
      const auto length = right.length();
      const auto right_begin = right_offset;
      const auto right_end = right_begin + length;
      right_offset += length;
      const auto typed_right = right.as<bool_type>();
      if (not typed_right) {
        diagnostic::warning("expected `bool`, but got `{}`", right.type.kind())
          .primary(x.right)
          .hint("this will be treated as `false`")
          .emit(self.ctx());
        return self.slice(right_begin, right_end).eval(fallback);
      }
      if (typed_right->array->true_count() == length) {
        return self.slice(right_begin, right_end).eval(x.left);
      }
      if (typed_right->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(x.right)
          .hint("use `else` to provide a fallback value")
          .emit(self.ctx());
      }
      if (typed_right->array->true_count() == 0) {
        return self.slice(right_begin, right_end).eval(fallback);
      }
      const auto get_right = [&](int64_t i) -> bool {
        return typed_right->array->IsValid(i)
               and typed_right->array->GetView(i);
      };
      auto result = multi_series{};
      auto range_offset = int64_t{0};
      auto range_current = get_right(0);
      const auto append_until = [&](int64_t end) {
        result.append(self.slice(right_begin + range_offset, right_begin + end)
                        .eval(range_current ? x.left : fallback));
      };
      for (auto i = int64_t{1}; i < length; ++i) {
        if (range_current == get_right(i)) {
          continue;
        }
        append_until(i);
        range_offset = i;
        range_current = not range_current;
      }
      append_until(length);
      TENZIR_ASSERT_EQ(result.length(), length);
      return result;
    });
}

auto eval_else(evaluator& self, const ast::binary_expr& x) -> multi_series {
  // Short-circuit the evaluation of `x if y else z`, avoiding the
  // construction of null series. This is also important for correctness, as
  // `null if true else 42` should return `null`, but without this would return
  // `42`.
  if (const auto* binop = try_as<ast::binary_expr>(x.left)) {
    if (binop->op.inner == ast::binary_op::if_) {
      return eval_if(self, *binop, x.right);
    }
  }
  if (disable_short_circuiting()) {
    auto inputs = std::array<multi_series, 2>{
      self.eval(x.left),
      self.eval(x.right),
    };
    return map_series(inputs, [&](std::span<series> parts) -> multi_series {
      TENZIR_ASSERT_EQ(parts.size(), 2u);
      auto& left = parts[0];
      auto& right = parts[1];
      const auto length = left.length();
      TENZIR_ASSERT_EQ(length, right.length());
      if (left.array->null_count() == 0) {
        return left;
      }
      if (left.array->null_count() == length) {
        return right;
      }
      auto result = multi_series{};
      auto range_offset = int64_t{0};
      auto range_current = left.array->IsValid(0);
      const auto append_until = [&](int64_t end) {
        if (range_current) {
          result.append(left.slice(range_offset, end));
        } else {
          result.append(right.slice(range_offset, end));
        }
      };
      for (auto i = int64_t{1}; i < length; ++i) {
        if (range_current == left.array->IsValid(i)) {
          continue;
        }
        append_until(i);
        range_offset = i;
        range_current = not range_current;
      }
      append_until(length);
      TENZIR_ASSERT_EQ(result.length(), length);
      return result;
    });
  }
  auto left_offset = int64_t{0};
  return map_series(self.eval(x.left), [&](series left) -> multi_series {
    const auto length = left.length();
    const auto left_begin = left_offset;
    const auto left_end = left_begin + length;
    left_offset += length;
    if (left.array->null_count() == 0) {
      return left;
    }
    if (left.array->null_count() == length) {
      return self.slice(left_begin, left_end).eval(x.right);
    }
    const auto get_left_valid = [&](int64_t i) -> bool {
      return left.array->IsValid(i);
    };
    auto result = multi_series{};
    auto range_offset = int64_t{0};
    auto range_current = get_left_valid(0);
    const auto append_until = [&](int64_t end) {
      if (not range_current) {
        result.append(
          self.slice(left_begin + range_offset, left_begin + end).eval(x.right));
        return;
      }
      result.append(left.slice(range_offset, end));
    };
    for (auto i = int64_t{1}; i < length; ++i) {
      if (range_current == get_left_valid(i)) {
        continue;
      }
      append_until(i);
      range_offset = i;
      range_current = not range_current;
    }
    append_until(length);
    TENZIR_ASSERT_EQ(result.length(), length);
    return result;
  });
}

} // namespace

auto evaluator::eval(const ast::binary_expr& x) -> multi_series {
  switch (x.op.inner) {
#define X(op)                                                                  \
  case ast::binary_op::op:                                                     \
    return eval_op<ast::binary_op::op>(*this, x)
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
      return eval_and_or<ast::binary_op::and_>(*this, x);
    case ast::binary_op::or_:
      return eval_and_or<ast::binary_op::or_>(*this, x);
    case ast::binary_op::if_:
      return eval_if(*this, x);
    case ast::binary_op::else_:
      return eval_else(*this, x);
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
