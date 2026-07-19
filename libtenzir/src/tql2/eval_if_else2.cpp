//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval_impl2.hpp"
#include "tenzir2/memory/shared_owner.hpp"
#include "tenzir2/type_system/array/builder.hpp"
#include "tenzir2/type_system/array/fundamental.hpp"

#include <vector>

namespace tenzir2 {

// ---------------------------------------------------------------------------
// append_to_builder: copy a single row view into an array_builder_<data>.
// ---------------------------------------------------------------------------

namespace {

/// Builds a `state_buffer` of length `length` where `pred(i)` selects
/// `element_state::valid` (active) vs. `element_state::dead` (inactive).
template <class Pred>
auto build_active_state(std::ptrdiff_t length, Pred&& pred)
  -> memory::detail::state_buffer {
  auto builder
    = memory::shared_owner<memory::element_state[]>::builder{
      memory::default_resource()};
  for (auto i = std::ptrdiff_t{0}; i < length; ++i) {
    builder.emplace_back(pred(i) ? memory::element_state::valid
                                 : memory::element_state::dead);
  }
  return memory::detail::state_buffer{builder.finish()};
}

auto append_to_builder(array_builder_<data>& b, array_row_view_<data> row)
  -> void {
  match(row, [&]<data_type T>(array_row_view_<T> typed) {
    if (not typed.valid()) {
      b.null(typed.state());
      return;
    }
    if constexpr (std::same_as<T, null>) {
      b.null();
    } else if constexpr (std::same_as<T, std::string>) {
      b.data(std::string{*typed});
    } else if constexpr (fundamental_type<T>) {
      b.data(*typed);
    } else {
      // list and record: not yet supported — produce null
      b.null();
    }
  });
}

} // namespace

// ---------------------------------------------------------------------------
// eval_if
//
// Evaluates `x.left if x.right else fallback`.
//
// Short-circuit semantics:
//   - then-branch is evaluated only for rows where cond is true.
//   - else-branch is evaluated only for rows where cond is false/null/inactive.
// ---------------------------------------------------------------------------

auto eval_if(evaluator& self, tenzir::ast::binary_expr const& x,
             tenzir::ast::expression const& fallback,
             ActiveRows const& active) -> array_<data> {
  auto cond = self.eval(x.right, active);

  // cond_active[i] = active[i] AND cond[i] is valid true bool.
  auto cond_active = std::vector<bool>(static_cast<std::size_t>(self.length()));
  auto warned_cond_type = false;
  auto warned_cond_null = false;

  for (auto i = std::ptrdiff_t{0}; i < self.length(); ++i) {
    auto cv = cond.get(i);
    auto active_row = active.is_active(i);
    if (auto* b = try_as<array_row_view_<bool>>(&cv)) {
      if (b->valid()) {
        cond_active[static_cast<std::size_t>(i)] = active_row and **b;
      } else {
        // null bool condition
        if (not warned_cond_null and active_row) {
          warned_cond_null = true;
          tenzir::diagnostic::warning("expected `bool`, but got `null`")
            .primary(x.right)
            .hint("use `else` to provide a fallback value")
            .emit(self.ctx());
        }
      }
    } else {
      // Non-bool condition
      if (not warned_cond_type and active_row and cv.valid()) {
        warned_cond_type = true;
        auto kind
          = match(cv, []<data_type T>(array_row_view_<T>) -> std::string_view {
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
              } else {
                return "record";
              }
            });
        tenzir::diagnostic::warning("expected `bool`, but got `{}`", kind)
          .primary(x.right)
          .hint("this will be treated as `false`")
          .emit(self.ctx());
      }
    }
  }

  auto then_state = build_active_state(self.length(), [&](std::ptrdiff_t i) {
    return cond_active[static_cast<std::size_t>(i)];
  });
  auto then_active = ActiveRows{then_state};

  // Build else_active: active[i] AND NOT cond_active[i].
  auto else_active = [&]() -> ActiveRows {
    if (active.as_constant() == true) {
      // All rows were active; then_state already encodes cond truthiness,
      // so inverted=true correctly marks then-rows inactive for the else
      // branch.
      return ActiveRows{then_state, true};
    }
    auto else_state = build_active_state(self.length(), [&](std::ptrdiff_t i) {
      return active.is_active(i) and not cond_active[static_cast<std::size_t>(i)];
    });
    return ActiveRows{std::move(else_state)};
  }();

  auto then_result = self.eval_narrowed(x.left, then_active);
  auto else_result = self.eval_narrowed(fallback, else_active);

  // Combine: cond_active[row]=true → then; false → else.
  auto result_builder = array_builder_<data>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < self.length(); ++row) {
    if (cond_active[static_cast<std::size_t>(row)]) {
      append_to_builder(result_builder, then_result.get(row));
    } else {
      append_to_builder(result_builder, else_result.get(row));
    }
  }

  return result_builder.finish();
}

auto eval_if(evaluator& self, tenzir::ast::binary_expr const& x,
             ActiveRows const& active) -> array_<data> {
  return eval_if(self, x,
                 tenzir::ast::constant{caf::none, tenzir::location::unknown},
                 active);
}

// ---------------------------------------------------------------------------
// eval_else
//
// Evaluates `x.left else x.right`: returns left where non-null, else right.
//
// Short-circuits `x if y else z` into a single pass when the left side is an
// `if_` expression, preserving correct null semantics for the then-branch.
// ---------------------------------------------------------------------------

auto eval_else(evaluator& self, tenzir::ast::binary_expr const& x,
               ActiveRows const& active) -> array_<data> {
  // Short-circuit: `(a if b) else c` → eval_if(a, b, c, active)
  if (auto const* binop = tenzir::try_as<tenzir::ast::binary_expr>(x.left)) {
    if (binop->op == tenzir::ast::binary_op::if_) {
      return eval_if(self, *binop, x.right, active);
    }
  }

  auto left = self.eval(x.left, active);

  // right_active[i] = active[i] AND left[i] is null.
  auto right_active_state
    = build_active_state(self.length(), [&](std::ptrdiff_t i) {
        auto lv = left.get(i);
        return active.is_active(i) and not lv.valid();
      });
  auto right_active = ActiveRows{std::move(right_active_state)};

  auto right = self.eval_narrowed(x.right, right_active);

  // Combine: left non-null → left; left null → right.
  auto result_builder = array_builder_<data>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < self.length(); ++row) {
    auto lv = left.get(row);
    if (lv.valid()) {
      append_to_builder(result_builder, lv);
    } else {
      append_to_builder(result_builder, right.get(row));
    }
  }

  return result_builder.finish();
}

} // namespace tenzir2
