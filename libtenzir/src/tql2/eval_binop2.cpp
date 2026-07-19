//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/eval_impl2.hpp"
#include "tenzir2/type_system/array/access.hpp"
#include "tenzir2/type_system/array/builder.hpp"
#include "tenzir2/type_system/array/fundamental.hpp"
#include "tenzir2/type_system/array/list.hpp"
#include "tenzir2/type_system/array/subnet.hpp"

#include <concepts>
#include <limits>
#include <optional>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace tenzir2 {

// Forward declarations for helpers in sibling translation units.
template <tenzir::ast::binary_op Op>
auto eval_and_or(evaluator& self, tenzir::ast::binary_expr const& x,
                 ActiveRows const& active) -> array_<data>;

auto eval_if(evaluator& self, tenzir::ast::binary_expr const& x,
             tenzir::ast::expression const& fallback,
             ActiveRows const& active) -> array_<data>;
auto eval_if(evaluator& self, tenzir::ast::binary_expr const& x,
             ActiveRows const& active) -> array_<data>;
auto eval_else(evaluator& self, tenzir::ast::binary_expr const& x,
               ActiveRows const& active) -> array_<data>;

namespace {

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

template <typename T>
concept integral_type2
  = std::same_as<T, std::int64_t> or std::same_as<T, std::uint64_t>;

template <typename T>
concept numeric_type2 = integral_type2<T> or std::same_as<T, double>;

[[maybe_unused]] constexpr auto is_arithmetic(tenzir::ast::binary_op op)
  -> bool {
  using enum tenzir::ast::binary_op;
  switch (op) {
    case add:
    case sub:
    case mul:
    case div:
      return true;
    default:
      return false;
  }
}

[[maybe_unused]] constexpr auto is_relational(tenzir::ast::binary_op op)
  -> bool {
  using enum tenzir::ast::binary_op;
  switch (op) {
    case eq:
    case neq:
    case gt:
    case lt:
    case geq:
    case leq:
      return true;
    default:
      return false;
  }
}

constexpr auto result_if_both_null(tenzir::ast::binary_op op)
  -> std::optional<bool> {
  using enum tenzir::ast::binary_op;
  switch (op) {
    case eq:
    case geq:
    case leq:
      return true;
    case neq:
      return false;
    default:
      return std::nullopt;
  }
}

template <data_type T>
constexpr auto data_type_name() -> std::string_view {
  if constexpr (std::same_as<T, null>) {
    return "null";
  } else if constexpr (std::same_as<T, bool>) {
    return "bool";
  } else if constexpr (std::same_as<T, std::int64_t>) {
    return "int64";
  } else if constexpr (std::same_as<T, std::uint64_t>) {
    return "uint64";
  } else if constexpr (std::same_as<T, double>) {
    return "double";
  } else if constexpr (std::same_as<T, std::string>) {
    return "string";
  } else if constexpr (std::same_as<T, ip>) {
    return "ip";
  } else if constexpr (std::same_as<T, subnet>) {
    return "subnet";
  } else if constexpr (std::same_as<T, time>) {
    return "time";
  } else if constexpr (std::same_as<T, duration>) {
    return "duration";
  } else if constexpr (std::same_as<T, list>) {
    return "list";
  } else if constexpr (std::same_as<T, record>) {
    return "record";
  } else {
    return "<?>";
  }
}

auto make_null_array(std::ptrdiff_t length, memory::element_state state
                                            = memory::element_state::null)
  -> array_<data> {
  auto builder = array_builder_<data>{};
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    builder.null(state);
  }
  return builder.finish();
}

auto has_active_rows(ActiveRows const& active, std::ptrdiff_t length)
  -> bool {
  if (auto constant = active.as_constant()) {
    return *constant;
  }
  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    if (active.is_active(static_cast<int64_t>(row))) {
      return true;
    }
  }
  return false;
}

auto contains_ip(subnet const& network, ip const& address) -> bool {
  auto bits = network.length;
  if (bits > 128) {
    return false;
  }
  auto lhs = network.ip.data();
  auto rhs = address.data();
  auto bytes = bits / 8;
  auto tail = static_cast<uint8_t>(bits % 8);
  if (not std::equal(lhs.begin(), lhs.begin() + bytes, rhs.begin())) {
    return false;
  }
  if (tail == 0) {
    return true;
  }
  auto mask = static_cast<uint8_t>(0xFFu << (8u - tail));
  return (std::to_integer<uint8_t>(lhs[bytes]) & mask)
         == (std::to_integer<uint8_t>(rhs[bytes]) & mask);
}

// View type used when dereferencing a typed row view. For strings the view
// type is std::string_view; for all other fundamental types it is T itself.
template <data_type T>
struct view_type_for {
  using type = T;
};
template <>
struct view_type_for<std::string> {
  using type = std::string_view;
};
template <data_type T>
using view_type_for_t = typename view_type_for<T>::type;

// ---------------------------------------------------------------------------
// BinOpKernel<Op, L, R> – value-level kernel for each supported type pair.
//
// Each instantiation provides:
//   using result = <result-value-type>;
//   static auto evaluate(view_type_for_t<L>, view_type_for_t<R>)
//     -> std::variant<result, const char*>;
//
// The primary template is empty (= "not implemented"); specialisations below
// provide the actual work. has_bin_op_kernel<Op,L,R> then checks for result.
// ---------------------------------------------------------------------------

template <tenzir::ast::binary_op Op, data_type L, data_type R>
struct BinOpKernel {};

// ---- Arithmetic: integer × integer (add / sub / mul) ----------------------

template <tenzir::ast::binary_op Op, data_type L, data_type R>
  requires(integral_type2<L> and integral_type2<R> and is_arithmetic(Op)
           and Op != tenzir::ast::binary_op::div)
struct BinOpKernel<Op, L, R> {
  static auto inner(L l, R r) {
    using enum tenzir::ast::binary_op;
    if constexpr (Op == add) {
      return tenzir::checked_add(l, r);
    } else if constexpr (Op == sub) {
      return tenzir::checked_sub(l, r);
    } else {
      static_assert(Op == mul);
      return tenzir::checked_mul(l, r);
    }
  }

  using result
    = decltype(inner(std::declval<L>(), std::declval<R>()))::value_type;

  static auto evaluate(L l, R r) -> std::variant<result, const char*> {
    auto res = inner(l, r);
    if (not res) {
      return "integer overflow";
    }
    return *res;
  }
};

// ---- Arithmetic: floating-point (add / sub / mul, at least one double) ----

template <tenzir::ast::binary_op Op, data_type L, data_type R>
  requires(numeric_type2<L> and numeric_type2<R> and is_arithmetic(Op)
           and Op != tenzir::ast::binary_op::div
           and (std::same_as<double, L> or std::same_as<double, R>))
struct BinOpKernel<Op, L, R> {
  using result = double;

  static auto evaluate(L l, R r) -> std::variant<double, const char*> {
    using enum tenzir::ast::binary_op;
    if constexpr (Op == add) {
      return static_cast<double>(l) + static_cast<double>(r);
    } else if constexpr (Op == sub) {
      return static_cast<double>(l) - static_cast<double>(r);
    } else {
      static_assert(Op == mul);
      return static_cast<double>(l) * static_cast<double>(r);
    }
  }
};

// ---- Division (all numeric) → double ---------------------------------------

template <numeric_type2 L, numeric_type2 R>
struct BinOpKernel<tenzir::ast::binary_op::div, L, R> {
  using result = double;

  static auto evaluate(L l, R r) -> std::variant<double, const char*> {
    if (r == R{}) {
      return "division by zero";
    }
    return static_cast<double>(l) / static_cast<double>(r);
  }
};

// ---- String concatenation ---------------------------------------------------

template <>
struct BinOpKernel<tenzir::ast::binary_op::add, std::string, std::string> {
  using result = std::string;

  static auto evaluate(std::string_view l, std::string_view r)
    -> std::variant<std::string, const char*> {
    return std::string{l} + std::string{r};
  }
};

// ---- Time / duration arithmetic --------------------------------------------

template <>
struct BinOpKernel<tenzir::ast::binary_op::sub, time, duration> {
  using result = time;
  static auto evaluate(time l, duration r) -> std::variant<time, const char*> {
    return time{l - r};
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::add, time, duration> {
  using result = time;
  static auto evaluate(time l, duration r) -> std::variant<time, const char*> {
    return time{l + r};
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::add, duration, time> {
  using result = time;
  static auto evaluate(duration l, time r) -> std::variant<time, const char*> {
    return time{l + r};
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::sub, time, time> {
  using result = duration;
  static auto evaluate(time l, time r) -> std::variant<duration, const char*> {
    return duration{l - r};
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::add, duration, duration> {
  using result = duration;
  static auto evaluate(duration l, duration r)
    -> std::variant<duration, const char*> {
    if (auto check = tenzir::checked_add(l.count(), r.count())) {
      return duration{std::chrono::nanoseconds{*check}};
    }
    return "duration addition overflow";
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::sub, duration, duration> {
  using result = duration;
  static auto evaluate(duration l, duration r)
    -> std::variant<duration, const char*> {
    if (auto check = tenzir::checked_sub(l.count(), r.count())) {
      return duration{std::chrono::nanoseconds{*check}};
    }
    return "duration subtraction overflow";
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::div, duration, duration> {
  using result = double;
  static auto evaluate(duration l, duration r)
    -> std::variant<double, const char*> {
    if (r.count() == 0) {
      return "division by zero";
    }
    return static_cast<double>(l.count()) / static_cast<double>(r.count());
  }
};

// duration * integer
template <integral_type2 N>
struct BinOpKernel<tenzir::ast::binary_op::mul, duration, N> {
  using result = duration;
  static auto evaluate(duration l, N r) -> std::variant<duration, const char*> {
    if (auto check = tenzir::checked_mul(l.count(), r)) {
      return duration{std::chrono::nanoseconds{*check}};
    }
    return "duration multiplication overflow";
  }
};

// integer * duration (commutative)
template <integral_type2 N>
struct BinOpKernel<tenzir::ast::binary_op::mul, N, duration> {
  using result = duration;
  static auto evaluate(N l, duration r) -> std::variant<duration, const char*> {
    return BinOpKernel<tenzir::ast::binary_op::mul, duration, N>::evaluate(r,
                                                                           l);
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::mul, duration, double> {
  using result = duration;
  static auto evaluate(duration l, double r)
    -> std::variant<duration, const char*> {
    return duration{std::chrono::nanoseconds{
      static_cast<duration::rep>(static_cast<double>(l.count()) * r)}};
  }
};

template <>
struct BinOpKernel<tenzir::ast::binary_op::mul, double, duration> {
  using result = duration;
  static auto evaluate(double l, duration r)
    -> std::variant<duration, const char*> {
    return BinOpKernel<tenzir::ast::binary_op::mul, duration, double>::evaluate(
      r, l);
  }
};

// duration / numeric
template <numeric_type2 N>
struct BinOpKernel<tenzir::ast::binary_op::div, duration, N> {
  using result = duration;
  static auto evaluate(duration l, N r) -> std::variant<duration, const char*> {
    if (r == N{}) {
      return "division by zero";
    }
    return duration{std::chrono::nanoseconds{static_cast<duration::rep>(
      static_cast<double>(l.count()) / static_cast<double>(r))}};
  }
};

// ---- Relational: integer × integer (use std::cmp_* for mixed signs) --------

template <tenzir::ast::binary_op Op, data_type L, data_type R>
  requires(integral_type2<L> and integral_type2<R> and is_relational(Op))
struct BinOpKernel<Op, L, R> {
  using result = bool;

  static auto evaluate(L l, R r) -> std::variant<bool, const char*> {
    using enum tenzir::ast::binary_op;
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
    } else {
      static_assert(Op == leq);
      return std::cmp_less_equal(l, r);
    }
  }
};

// ---- Relational: general comparable types (not int×int) --------------------

template <tenzir::ast::binary_op Op, data_type L, data_type R>
  requires(
    is_relational(Op) and not(integral_type2<L> and integral_type2<R>)
    and not std::same_as<L, null> and not std::same_as<R, null>
    and requires(view_type_for_t<L> l, view_type_for_t<R> r) { l <=> r; })
struct BinOpKernel<Op, L, R> {
  using result = bool;

  static auto evaluate(view_type_for_t<L> l, view_type_for_t<R> r)
    -> std::variant<bool, const char*> {
    using enum tenzir::ast::binary_op;
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
    } else {
      static_assert(Op == leq);
      return l <= r;
    }
  }
};

// ---- In: string substring check --------------------------------------------

template <>
struct BinOpKernel<tenzir::ast::binary_op::in, std::string, std::string> {
  using result = bool;
  static auto evaluate(std::string_view l, std::string_view r)
    -> std::variant<bool, const char*> {
    return r.find(l) != std::string_view::npos;
  }
};

// ---- In: IP in subnet ------------------------------------------------------

template <>
struct BinOpKernel<tenzir::ast::binary_op::in, ip, subnet> {
  using result = bool;
  static auto evaluate(ip l, subnet r) -> std::variant<bool, const char*> {
    return contains_ip(r, l);
  }
};

// ---- In: subnet in subnet --------------------------------------------------

template <>
struct BinOpKernel<tenzir::ast::binary_op::in, subnet, subnet> {
  using result = bool;
  static auto evaluate(subnet l, subnet r) -> std::variant<bool, const char*> {
    // l is in r when r covers all addresses of l:
    // r.length <= l.length AND l.ip is inside r's network.
    return r.length <= l.length and contains_ip(r, l.ip);
  }
};

// ---------------------------------------------------------------------------
// has_bin_op_kernel concept
// ---------------------------------------------------------------------------

template <tenzir::ast::binary_op Op, typename L, typename R>
concept has_bin_op_kernel
  = requires { typename BinOpKernel<Op, L, R>::result; };

// ---------------------------------------------------------------------------
// handle_in_list: membership test for `lv in list_rv`.
//
// Called before the general null handling in eval_op so that `null in list`
// correctly returns true when the list contains a null element.
// ---------------------------------------------------------------------------

auto handle_in_list(array_row_view_<data> lv, array_row_view_<list> list_rv,
                    array_builder_<data>& builder) -> void {
  if (not list_rv.valid()) {
    builder.null(list_rv.state());
    return;
  }
  // null in list → true iff the list contains at least one null element
  if (not lv.valid()) {
    auto found = false;
    for (auto elem : *list_rv) {
      if (not elem.valid()) {
        found = true;
        break;
      }
    }
    builder.data(found);
    return;
  }
  // lv is a valid value – check element-wise using the eq kernel
  auto found = false;
  match(lv, [&]<data_type L>(array_row_view_<L> l_row) {
    for (auto elem : *list_rv) {
      if (not elem.valid()) {
        continue;
      }
      match(elem, [&]<data_type R>(array_row_view_<R> e_row) {
        if constexpr (has_bin_op_kernel<tenzir::ast::binary_op::eq, L, R>) {
          using kernel = BinOpKernel<tenzir::ast::binary_op::eq, L, R>;
          auto res = kernel::evaluate(*l_row, *e_row);
          if (auto* val = std::get_if<bool>(&res); val and *val) {
            found = true;
          }
        }
      });
      if (found) {
        break;
      }
    }
  });
  builder.data(found);
}

// ---------------------------------------------------------------------------
// eval_op<Op>: row-by-row binary operation with full type dispatch.
// ---------------------------------------------------------------------------

template <tenzir::ast::binary_op Op>
auto eval_op(evaluator& self, tenzir::ast::binary_expr const& x,
             ActiveRows const& active) -> array_<data> {
  TENZIR_ASSERT_EQ(x.op, Op);
  auto left = self.eval(x.left, active);
  auto right = self.eval(x.right, active);
  auto length = self.length();

  auto builder = array_builder_<data>{};

  // Kernel-level error messages (deduplicated, emitted after the loop).
  auto kernel_errors = std::vector<const char*>{};
  // First unsupported type pair (emitted once after the loop).
  auto unsupported_l = std::string_view{};
  auto unsupported_r = std::string_view{};

  for (auto row = std::ptrdiff_t{0}; row < length; ++row) {
    if (not active.is_active(static_cast<int64_t>(row))) {
      builder.null(memory::element_state::dead);
      continue;
    }
    auto lv = left.get(row);
    auto rv = right.get(row);

    // Special case: `in list` needs different null semantics (null in list may
    // return true), so handle it before the general null checks below.
    if constexpr (Op == tenzir::ast::binary_op::in) {
      if (auto* list_rv = try_as<array_row_view_<list>>(&rv)) {
        handle_in_list(lv, *list_rv, builder);
        continue;
      }
    }

    auto l_null = not lv.valid();
    auto r_null = not rv.valid();

    if (l_null and r_null) {
      constexpr auto res = result_if_both_null(Op);
      if constexpr (res.has_value()) {
        builder.data(*res);
      } else {
        builder.null();
      }
      continue;
    }

    if (l_null or r_null) {
      if constexpr (Op == tenzir::ast::binary_op::eq) {
        builder.data(false);
      } else if constexpr (Op == tenzir::ast::binary_op::neq) {
        builder.data(true);
      } else {
        builder.null();
      }
      continue;
    }

    // Both operands are valid – double-dispatch on their concrete types.
    auto handled = match(lv, [&]<data_type L>(array_row_view_<L> l_row) -> bool {
      return match(rv, [&]<data_type R>(array_row_view_<R> r_row) -> bool {
        if constexpr (has_bin_op_kernel<Op, L, R>) {
          using kernel = BinOpKernel<Op, L, R>;
          auto res = kernel::evaluate(*l_row, *r_row);
          if (auto* val = std::get_if<typename kernel::result>(&res)) {
            if constexpr (std::same_as<typename kernel::result, std::string>) {
              builder.data(std::string{*val});
            } else {
              builder.data(*val);
            }
          } else {
            auto* err = std::get<const char*>(res);
            if (std::ranges::find(kernel_errors, err) == kernel_errors.end()) {
              kernel_errors.push_back(err);
            }
            builder.null();
          }
          return true;
        } else {
          if (unsupported_l.empty()) {
            unsupported_l = data_type_name<L>();
            unsupported_r = data_type_name<R>();
          }
          return false;
        }
      });
    });

    if (not handled) {
      builder.null();
    }
  }

  for (auto* err : kernel_errors) {
    tenzir::diagnostic::warning("{}", err).primary(x).emit(self.ctx());
  }
  if (not unsupported_l.empty()
      and has_active_rows(active, static_cast<std::ptrdiff_t>(length))) {
    tenzir::diagnostic::warning("binary operator `{}` not implemented for `{}` "
                                "and `{}`",
                                x.op, unsupported_l, unsupported_r)
      .primary(x)
      .hint("the result of this expression is `null`")
      .emit(self.ctx());
  }

  return builder.finish();
}

} // namespace

// ---------------------------------------------------------------------------
// evaluator::eval(binary_expr)
// ---------------------------------------------------------------------------

auto evaluator::eval(tenzir::ast::binary_expr const& x,
                     ActiveRows const& active) -> array_<data> {
  using enum tenzir::ast::binary_op;
#define X(op)                                                                  \
  case op:                                                                     \
    return eval_op<op>(*this, x, active)
  switch (x.op) {
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
    case and_:
      return eval_and_or<and_>(*this, x, active);
    case or_:
      return eval_and_or<or_>(*this, x, active);
    case if_:
      return eval_if(*this, x, active);
    case else_:
      return eval_else(*this, x, active);
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir2
