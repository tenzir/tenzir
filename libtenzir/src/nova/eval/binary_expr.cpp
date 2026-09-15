#include "tenzir/checked_math.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/nova/array_merge.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/comparison.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/list_array.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <chrono>
#include <concepts>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tenzir::nova {

namespace {

/// Builds a new `Array<Data>` taking row `i` from `a` when it is selected by
/// `mask_a` and from `b` when selected by `mask_b`. The two masks must be
/// disjoint. Rows outside both masks are not guaranteed to have valid storage
/// in either `a` or `b`, so they are never read and their result values are
/// unspecified; callers only ever expose rows inside `mask_a | mask_b`.
auto select_rows(const Array<Data>& a, storage::BitMap const& mask_a,
                 const Array<Data>& b, storage::BitMap const& mask_b)
  -> Array<Data> {
  TENZIR_ASSERT_EQ(a.length(), mask_a.length());
  TENZIR_ASSERT_EQ(b.length(), mask_b.length());
  TENZIR_ASSERT_EQ(a.length(), b.length());
  TENZIR_ASSERT_EXPENSIVE(not(mask_a & mask_b).any());
  if (not mask_b.any()) {
    return a;
  }
  if (not mask_a.any()) {
    return b;
  }
  // `with_merged` works column-wise on the physical storage: same-typed
  // fundamental arrays are merged by copying one and patching the other's
  // rows, records are merged field by field, and differently-typed arrays
  // become a `UnionArray` without any per-row variant dispatch.
  return with_merged(MaskedArray<Array<Data>>{a, mask_a},
                     MaskedArray<Array<Data>>{b, mask_b});
}

/// Equality compares `null` by nullness. The ordering operators have no order
/// to report, so they do not accept it at all and `apply_kernel` warns.
template <class U, class T, ast::binary_op Op>
concept null_compared_with
  = not _::is_ordering(Op) and (std::same_as<T, Null> or std::same_as<U, Null>);

template <class U, class T>
concept other_number_than = concepts::number<U> and not std::same_as<T, U>;

/// Equality accepts any two operands of the same fundamental type, the
/// ordering operators only the ordered ones. `Null` belongs to
/// `null_compared_with`.
template <class T, ast::binary_op Op>
concept self_comparable
  = fundamental_view_type<T> and not std::same_as<T, Null>
    and (not _::is_ordering(Op)
         or concepts::one_of<T, Int, UInt, Float, Time, Duration>);

/// The rows must stay mutually exclusive: `apply_kernel` probes them with
/// `std::is_invocable_v`, which is `false` for an ambiguous call, so an
/// overlap silently drops a type pair instead of failing to compile.
template <ast::binary_op Op>
auto comparison_kernel() {
  return ::tenzir::detail::overload{
    []<class T, null_compared_with<T, Op> U>(diagnostic_handler&, T,
                                             U) -> Option<Bool> {
      return compare<Op>(std::same_as<T, Null>, std::same_as<U, Null>);
    },
    []<concepts::number T, other_number_than<T> U>(diagnostic_handler&, T lhs,
                                                   U rhs) -> Option<Bool> {
      return compare<Op>(lhs, rhs);
    },
    []<self_comparable<Op> T>(diagnostic_handler&, T lhs,
                              T rhs) -> Option<Bool> {
      return compare<Op>(lhs, rhs);
    },
  };
}

template <ast::binary_op Op>
auto eval_comparison(const ast::binary_expr& x, std::string_view name,
                     EvalFrame frame) -> Array<Data> {
  return apply_kernel<2>(frame, name, {x.left, x.right}, x.get_location(),
                         comparison_kernel<Op>());
}

/// Returns whether `list` contains an element equal to `lhs`. A `Null` on the
/// left-hand side matches only `Null` elements, and `Int`/`UInt` compare by
/// value across signedness. Elements of other fundamental types never match.
/// Encountering a structured element (a nested list or record) emits a warning
/// attributed to `loc` and yields `None`.
template <fundamental_view_type T>
auto list_contains(diagnostic_handler& dh, WarnOnce& warn_unsupported,
                   location loc, T lhs, const RowView<List>& list)
  -> Option<Bool> {
  for (auto element : list) {
    auto const found = match(
      element, [&]<class V>(RowView<V> view) -> Option<Bool> {
        if constexpr (std::same_as<V, Null>) {
          return std::same_as<T, Null>;
        } else if constexpr (std::same_as<V, List> or std::same_as<V, Record>) {
          warn_unsupported(dh, diagnostic::warning("binary operator `in` does "
                                                   "not support "
                                                   "`{}` elements in the list",
                                                   Type<V>::static_name)
                                 .primary(loc));
          return None{};
        } else {
          using E = std::remove_cvref_t<decltype(*view)>;
          if constexpr (concepts::one_of<T, Int, UInt, Float>
                        and concepts::one_of<E, Int, UInt, Float>) {
            return compare<ast::binary_op::eq>(lhs, *view);
          } else if constexpr (requires() { lhs == *view; }
                               and not std::same_as<T, Null>) {
            return lhs == *view;
          } else {
            return false;
          }
        }
      });
    if (not found or *found) {
      return found;
    }
  }
  return false;
}

struct BooleanOperand {
  storage::BitMap true_rows;
  storage::BitMap false_rows;
};

/// Evaluates `expr` for `frame.mask()` and splits the active rows into
/// `true_rows`, `false_rows`, and the rest, which are `null`. Rows of a type
/// other than `bool` and `null` emit a warning and count as `null`.
auto eval_boolean_operand(EvalFrame frame, ast::expression const& expr)
  -> BooleanOperand {
  auto const& mask = frame.mask();
  auto value = frame.eval(expr);
  auto const length = value.length();
  auto bool_alt = value.get_alternative<Bool>();
  static_assert(Type<Bool>::PhysicalStorage::size == 1,
                "The code assumes bitmap is the only bool storage");
  auto bool_rows
    = bool_alt ? mask & bool_alt->present : storage::BitMap{length, false};
  auto true_rows = bool_alt
                     ? bool_rows & as<storage::BitMap>(bool_alt->data.storage())
                     : storage::BitMap{length, false};
  auto false_rows = std::move(bool_rows).and_not(true_rows);
  auto null_rows = mask.and_not(true_rows).and_not(false_rows);
  auto null_alt = value.get_alternative<Null>();
  auto invalid_rows = null_alt ? std::move(null_rows).and_not(null_alt->present)
                               : std::move(null_rows);
  if (invalid_rows.any()) {
    diagnostic::warning("expected `bool`, but got a different type")
      .primary(expr)
      .hint("the result of this expression is `null`")
      .emit(frame);
  }
  return {
    .true_rows = std::move(true_rows),
    .false_rows = std::move(false_rows),
  };
}

/// Three-valued `and`/`or`: `null and false` is `false`, `null or true` is
/// `true`, and every other combination with `null` is `null`.
auto eval_and_or(ast::binary_expr const& x, EvalFrame frame) -> Array<Data> {
  TENZIR_ASSERT(x.op == ast::binary_op::and_ or x.op == ast::binary_op::or_);
  auto const& mask = frame.mask();
  auto const length = frame.length();
  auto const is_and = x.op == ast::binary_op::and_;
  auto left = eval_boolean_operand(frame, x.left);
  // Short-circuiting is mask narrowing: the right operand is evaluated only
  // for the rows whose result it can still change, including `null` rows.
  auto right_mask
    = is_and ? mask.and_not(left.false_rows) : mask.and_not(left.true_rows);
  auto right
    = right_mask.any()
        ? eval_boolean_operand(frame.narrow(std::move(right_mask)), x.right)
        : BooleanOperand{storage::BitMap{length, false},
                         storage::BitMap{length, false}};
  // The right-hand bitmaps are all-false outside `right_mask`.
  auto true_rows = is_and ? left.true_rows & right.true_rows
                          : std::move(left.true_rows) | right.true_rows;
  auto false_rows = is_and ? std::move(left.false_rows) | right.false_rows
                           : left.false_rows & right.false_rows;
  auto null_rows = mask.and_not(true_rows).and_not(false_rows);
  return Array<Data>{Array<Bool>{std::move(true_rows)}}.null_where(
    std::move(null_rows));
}

auto eval_if(const ast::binary_expr& x, const ast::expression& fallback,
             EvalFrame frame) -> Array<Data>;

auto eval_if(const ast::binary_expr& x, EvalFrame frame) -> Array<Data> {
  return eval_if(x, ast::constant{caf::none, location::unknown},
                 std::move(frame));
}

auto eval_if(const ast::binary_expr& x, const ast::expression& fallback,
             EvalFrame frame) -> Array<Data> {
  TENZIR_ASSERT_EQ(x.op, ast::binary_op::if_);
  auto const& mask = frame.mask();
  auto cond = frame.eval(x.right);
  auto const length = cond.length();
  // `bool_alt->present` is "this row's condition alternative is `Bool`";
  // ANDing it with the underlying `Bool` values and `mask` selects exactly
  // the rows that are active, hold `Bool`, and are `true` -- no row-by-row
  // loop needed.
  auto bool_alt = cond.get_alternative<Bool>();
  static_assert(Type<Bool>::PhysicalStorage::size == 1,
                "The code assumes bitmap is the only bool storage");
  auto then_mask = bool_alt ? mask & bool_alt->present
                                & as<storage::BitMap>(bool_alt->data.storage())
                            : storage::BitMap{length, false};
  // Warn once if some active row's condition is neither `Bool` nor `Null`.
  auto bool_mask
    = bool_alt ? bool_alt->present : storage::BitMap{length, false};
  auto null_alt = cond.get_alternative<Null>();
  auto null_mask
    = null_alt ? null_alt->present : storage::BitMap{length, false};
  auto bad_type_mask = mask.and_not(bool_mask).and_not(null_mask);
  if (bad_type_mask.any()) {
    diagnostic::warning("expected `bool`, but got a different type")
      .primary(x.right)
      .hint("this will be treated as `false`")
      .emit(frame);
  }
  auto else_mask = mask.and_not(then_mask);
  auto then_val = frame.narrow(then_mask).eval(x.left);
  auto else_val = frame.narrow(else_mask).eval(fallback);
  auto combined = select_rows(then_val, then_mask, else_val, else_mask);
  return combined;
}

auto eval_else(const ast::binary_expr& x, EvalFrame frame) -> Array<Data> {
  TENZIR_ASSERT_EQ(x.op, ast::binary_op::else_);
  auto const& mask = frame.mask();
  // Short-circuit `x if y else z` (parsed as
  // `binary_expr{op=else_, left=binary_expr{op=if_, left=x, right=y},
  // right=z}`), avoiding ever materializing an intermediate all-null series
  // for the "no explicit else" default. This is also important for
  // correctness: `null if true else 42` must return `null`, but evaluating
  // `if_` on its own and then `else_`-ing the (all-null on non-matching rows)
  // result against `42` would return `42` instead.
  if (auto const* binop = try_as<ast::binary_expr>(x.left)) {
    if (binop->op == ast::binary_op::if_) {
      return eval_if(*binop, x.right, std::move(frame));
    }
  }
  auto left_val = frame.eval(x.left);
  auto const length = left_val.length();
  auto null_alt = left_val.get_alternative<Null>();
  auto is_left_null
    = mask & (null_alt ? null_alt->present : storage::BitMap{length, false});
  auto right_val = frame.narrow(is_left_null).eval(x.right);
  auto const take_left = mask.and_not(is_left_null);
  auto combined = select_rows(left_val, take_left, right_val, is_left_null);
  return combined;
}

} // namespace

auto _::EvalRun::eval(const ast::binary_expr& x, EvalFrame frame)
  -> Array<Data> {
  switch (x.op) {
    using enum ast::binary_op;
    case add: {
      auto warn_int_overflow = WarnOnce{};
      auto warn_duration_overflow = WarnOnce{};
      return apply_kernel<2>(
        frame, "binary operator `+`", {x.left, x.right}, x.get_location(),
        ::tenzir::detail::overload{
          [&x, &warn_int_overflow]<class T, class U>(diagnostic_handler& dh,
                                                     T lhs, U rhs)
            requires((std::same_as<T, Int> or std::same_as<T, UInt>
                      or std::same_as<T, Float>)
                     and (std::same_as<U, Int> or std::same_as<U, UInt>
                          or std::same_as<U, Float>))
          {
            if constexpr (std::same_as<T, Float> or std::same_as<U, Float>) {
              return Option{static_cast<Float>(lhs) + static_cast<Float>(rhs)};
            } else {
              auto result = checked_add(lhs, rhs);
              using ResultType = typename decltype(result)::value_type;
              if (not result) {
                warn_int_overflow(
                  dh, diagnostic::warning("integer overflow").primary(x));
                return Option<ResultType>{None{}};
              }
              return Option<ResultType>{*result};
            }
          },
          [](diagnostic_handler&, Time lhs, Duration rhs) -> Option<Time> {
            return lhs + rhs;
          },
          [](diagnostic_handler&, Duration lhs, Time rhs) -> Option<Time> {
            return lhs + rhs;
          },
          [&x, &warn_duration_overflow](diagnostic_handler& dh, Duration lhs,
                                        Duration rhs) -> Option<Duration> {
            auto result = checked_add(lhs.count(), rhs.count());
            if (not result) {
              warn_duration_overflow(
                dh,
                diagnostic::warning("duration addition overflow").primary(x));
              return None{};
            }
            return Duration{*result};
          },
        });
    }
    case sub: {
      auto warn_int_overflow = WarnOnce{};
      auto warn_duration_overflow = WarnOnce{};
      auto warn_time_overflow = WarnOnce{};
      return apply_kernel<2>(
        frame, "binary operator `-`", {x.left, x.right}, x.get_location(),
        ::tenzir::detail::overload{
          [&x, &warn_int_overflow]<class T, class U>(diagnostic_handler& dh,
                                                     T lhs, U rhs)
            requires((std::same_as<T, Int> or std::same_as<T, UInt>
                      or std::same_as<T, Float>)
                     and (std::same_as<U, Int> or std::same_as<U, UInt>
                          or std::same_as<U, Float>))
          {
            if constexpr (std::same_as<T, Float> or std::same_as<U, Float>) {
              return Option{static_cast<Float>(lhs) - static_cast<Float>(rhs)};
            } else {
              auto result = checked_sub(lhs, rhs);
              using ResultType = typename decltype(result)::value_type;
              if (not result) {
                warn_int_overflow(
                  dh, diagnostic::warning("integer overflow").primary(x));
                return Option<ResultType>{None{}};
              }
              return Option<ResultType>{*result};
            }
          },
          [](diagnostic_handler&, Time lhs, Duration rhs) -> Option<Time> {
            return lhs - rhs;
          },
          [&x, &warn_duration_overflow](diagnostic_handler& dh, Duration lhs,
                                        Duration rhs) -> Option<Duration> {
            auto result = checked_sub(lhs.count(), rhs.count());
            if (not result) {
              warn_duration_overflow(
                dh,
                diagnostic::warning("duration subtraction overflow").primary(x));
              return None{};
            }
            return Duration{*result};
          },
          [&x, &warn_time_overflow](diagnostic_handler& dh, Time lhs,
                                    Time rhs) -> Option<Duration> {
            auto result = checked_sub(lhs.time_since_epoch().count(),
                                      rhs.time_since_epoch().count());
            if (not result) {
              warn_time_overflow(
                dh,
                diagnostic::warning("time subtraction overflow").primary(x));
              return None{};
            }
            return Duration{*result};
          },
        });
    }
    case mul: {
      auto warn_int_overflow = WarnOnce{};
      auto warn_duration_overflow = WarnOnce{};
      auto mul_duration
        = [&x, &warn_duration_overflow](diagnostic_handler& dh, Duration lhs,
                                        auto rhs) -> Option<Duration> {
        using N = decltype(rhs);
        if constexpr (std::same_as<N, Float>) {
          return std::chrono::duration_cast<Duration>(lhs * rhs);
        } else {
          auto result = checked_mul(lhs.count(), rhs);
          if (not result) {
            warn_duration_overflow(
              dh, diagnostic::warning("duration multiplication overflow")
                    .primary(x));
            return None{};
          }
          return Duration{*result};
        }
      };
      return apply_kernel<2>(
        frame, "binary operator `*`", {x.left, x.right}, x.get_location(),
        ::tenzir::detail::overload{
          [&x, &warn_int_overflow]<class T, class U>(diagnostic_handler& dh,
                                                     T lhs, U rhs)
            requires((std::same_as<T, Int> or std::same_as<T, UInt>
                      or std::same_as<T, Float>)
                     and (std::same_as<U, Int> or std::same_as<U, UInt>
                          or std::same_as<U, Float>))
                    {
                      if constexpr (std::same_as<T, Float>
                                    or std::same_as<U, Float>) {
                        return Option{static_cast<Float>(lhs)
                                      * static_cast<Float>(rhs)};
                      } else {
                        auto result = checked_mul(lhs, rhs);
                        using ResultType =
                          typename decltype(result)::value_type;
                        if (not result) {
                          warn_int_overflow(
                            dh,
                            diagnostic::warning("integer overflow").primary(x));
                          return Option<ResultType>{None{}};
                        }
                        return Option<ResultType>{*result};
                      }
                    },
                    [&mul_duration]<class N>(diagnostic_handler& dh,
                                             Duration lhs,
                                             N rhs) -> Option<Duration>
                      requires(std::same_as<N, Int> or std::same_as<N, UInt>
                               or std::same_as<N, Float>)
                              {
                                return mul_duration(dh, lhs, rhs);
                              },
                              [&mul_duration]<class N>(
                                diagnostic_handler& dh, N lhs,
                                Duration rhs) -> Option<Duration>
                                requires(std::same_as<N, Int>
                                         or std::same_as<N, UInt>
                                         or std::same_as<N, Float>)
          {
            return mul_duration(dh, rhs, lhs);
          },
        });
    }
    case div: {
      auto warn_div_by_zero = WarnOnce{};
      return apply_kernel<2>(
        frame, "binary operator `/`", {x.left, x.right}, x.get_location(),
        ::tenzir::detail::overload{
          [&x, &warn_div_by_zero]<class T, class U>(
            diagnostic_handler& dh, T lhs, U rhs) -> Option<Float>
            requires((std::same_as<T, Int> or std::same_as<T, UInt>
                      or std::same_as<T, Float>)
                     and (std::same_as<U, Int> or std::same_as<U, UInt>
                          or std::same_as<U, Float>))
                    {
                      if (rhs == U{}) {
                        warn_div_by_zero(
                          dh,
                          diagnostic::warning("division by zero").primary(x));
                        return None{};
                      }
                      return static_cast<Float>(lhs) / static_cast<Float>(rhs);
                    },
                    [&x, &warn_div_by_zero](diagnostic_handler& dh,
                                            Duration lhs,
                                            Duration rhs) -> Option<Float> {
                      if (rhs.count() == 0) {
                        warn_div_by_zero(
                          dh,
                          diagnostic::warning("division by zero").primary(x));
                        return None{};
                      }
                      return static_cast<Float>(lhs.count())
                             / static_cast<Float>(rhs.count());
                    },
                    [&x, &warn_div_by_zero]<class N>(diagnostic_handler& dh,
                                                     Duration lhs,
                                                     N rhs) -> Option<Duration>
                      requires(std::same_as<N, Int> or std::same_as<N, UInt>
                               or std::same_as<N, Float>)
          {
            if (rhs == N{}) {
              warn_div_by_zero(
                dh, diagnostic::warning("division by zero").primary(x));
              return None{};
            }
            return std::chrono::duration_cast<Duration>(lhs / rhs);
          },
          });
    }
    case eq:
      return eval_comparison<eq>(x, "binary operator `==`", std::move(frame));
    case neq:
      return eval_comparison<neq>(x, "binary operator `!=`", std::move(frame));
    case gt:
      return eval_comparison<gt>(x, "binary operator `>`", std::move(frame));
    case geq:
      return eval_comparison<geq>(x, "binary operator `>=`", std::move(frame));
    case lt:
      return eval_comparison<lt>(x, "binary operator `<`", std::move(frame));
    case leq:
      return eval_comparison<leq>(x, "binary operator `<=`", std::move(frame));
    case and_:
    case or_:
      return eval_and_or(x, std::move(frame));
    case in: {
      auto warn_unsupported = WarnOnce{};
      return apply_kernel<2>(
        frame, "binary operator `in`", {x.left, x.right}, x.get_location(),
        ::tenzir::detail::overload{
          [](diagnostic_handler&, std::string_view lhs,
             std::string_view rhs) -> Option<Bool> {
            return rhs.find(lhs) != std::string_view::npos;
          },
          [](diagnostic_handler&, Ip lhs, Subnet rhs) -> Option<Bool> {
            return rhs.contains(lhs);
          },
          [](diagnostic_handler&, Subnet lhs, Subnet rhs) -> Option<Bool> {
            return rhs.contains(lhs);
          },
          [&x, &warn_unsupported]<fundamental_view_type T>(
            diagnostic_handler& dh, T lhs, RowView<List> rhs) -> Option<Bool> {
            return list_contains(dh, warn_unsupported, x.get_location(), lhs,
                                 rhs);
          },
        });
    }
    case if_:
      return eval_if(x, std::move(frame));
    case else_:
      return eval_else(x, std::move(frame));
  }
}

} // namespace tenzir::nova
