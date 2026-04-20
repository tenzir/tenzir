//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/eval_impl2.hpp"
#include "tenzir2/type_system/array/builder.hpp"
#include "tenzir2/type_system/array/fundamental.hpp"

#include <arrow/array/builder_primitive.h>

namespace tenzir2 {

// ---------------------------------------------------------------------------
// eval_and_or<Op>
//
// Implements three-valued AND / OR with short-circuit evaluation:
//
//   and: skip right evaluation where left is definitively false.
//   or:  skip right evaluation where left is definitively true.
//
// The result follows SQL-style three-valued logic:
//   AND: T&T=T, T&F=F, T&N=N, F&F=F, F&N=F, N&N=N
//   OR:  T|T=T, T|F=T, T|N=T, F|F=F, F|N=N, N|N=N
//
// Non-bool left / right values are treated as null.
// ---------------------------------------------------------------------------

template <tenzir::ast::binary_op Op>
auto eval_and_or(evaluator& self, tenzir::ast::binary_expr const& x,
                 tenzir::ActiveRows const& active) -> array_<data> {
  static_assert(Op == tenzir::ast::binary_op::and_
                or Op == tenzir::ast::binary_op::or_);

  auto left = self.eval(x.left, active);

  // Build left_flat: a flat BooleanArray; null for inactive or non-bool rows.
  auto left_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
  tenzir::check(left_builder.Reserve(self.length()));
  auto warned_left_type = false;

  for (auto row = std::ptrdiff_t{0}; row < self.length(); ++row) {
    auto lv = left.get(row);
    if (auto* bool_view = try_as<array_row_view_<bool>>(&lv)) {
      if (bool_view->valid()) {
        left_builder.UnsafeAppend(**bool_view);
      } else {
        left_builder.UnsafeAppendNull();
      }
      continue;
    }
    // Non-bool: warn once for active non-null rows
    if (not warned_left_type and lv.valid()
        and active.is_active(static_cast<int64_t>(row))) {
      warned_left_type = true;
      auto kind
        = match(lv, []<data_type T>(array_row_view_<T>) -> std::string_view {
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
        .primary(x.left)
        .hint("the result of this expression is `null`")
        .emit(self.ctx());
    }
    left_builder.UnsafeAppendNull();
  }

  auto left_flat = std::static_pointer_cast<arrow::BooleanArray>(
    tenzir::finish(left_builder));
  TENZIR_ASSERT_EQ(left_flat->length(), self.length());

  // For AND: rows where left is definitively false can skip right evaluation.
  // For OR:  rows where left is definitively true  can skip right evaluation.
  constexpr auto inactive_value = (Op == tenzir::ast::binary_op::or_);

  auto right_active = [&]() -> tenzir::ActiveRows {
    if (active.as_constant() == true) {
      return tenzir::ActiveRows{left_flat, inactive_value};
    }
    auto mask_builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
    tenzir::check(mask_builder.Reserve(self.length()));
    for (auto i = int64_t{0}; i < self.length(); ++i) {
      mask_builder.UnsafeAppend(
        active.is_active(i)
        and (left_flat->IsNull(i) or left_flat->GetView(i) != inactive_value));
    }
    return tenzir::ActiveRows{std::static_pointer_cast<arrow::BooleanArray>(
                                tenzir::finish(mask_builder)),
                              false};
  }();

  auto right = self.eval_narrowed(x.right, right_active);

  // Combine left_flat and right with three-valued logic.
  auto result_builder = array_builder_<bool>{memory::default_resource()};
  auto warned_right_type = false;

  for (auto row = std::ptrdiff_t{0}; row < self.length(); ++row) {
    auto global_i = static_cast<int64_t>(row);
    auto left_valid = left_flat->IsValid(global_i);
    auto left_true = left_valid and left_flat->GetView(global_i);
    auto left_false = left_valid and not left_flat->GetView(global_i);

    auto rv = right.get(row);
    auto* right_bool = try_as<array_row_view_<bool>>(&rv);
    auto right_val_valid = right_bool and right_bool->valid();
    auto right_val = right_val_valid ? **right_bool : false;

    // Warn once for active non-null non-bool right values.
    auto append_from_right = [&]() {
      if (right_val_valid) {
        result_builder.data(right_val);
        return;
      }
      // right is null or non-bool
      if (not right_bool and rv.valid() and not warned_right_type
          and right_active.is_active(global_i)) {
        warned_right_type = true;
        auto kind
          = match(rv, []<data_type T>(array_row_view_<T>) -> std::string_view {
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
          .hint("the result of this expression is `null`")
          .emit(self.ctx());
      }
      result_builder.null();
    };

    if constexpr (Op == tenzir::ast::binary_op::and_) {
      if (left_true) {
        append_from_right();
      } else if (left_false) {
        result_builder.data(false);
      } else {
        // left is null: result is false if right is definitely false, else null
        if (right_val_valid and not right_val) {
          result_builder.data(false);
        } else {
          result_builder.null();
        }
      }
    } else {
      static_assert(Op == tenzir::ast::binary_op::or_);
      if (left_true) {
        result_builder.data(true);
      } else if (left_false) {
        append_from_right();
      } else {
        // left is null: result is true if right is definitely true, else null
        if (right_val_valid and right_val) {
          result_builder.data(true);
        } else {
          result_builder.null();
        }
      }
    }
  }

  return result_builder.finish();
}

// Explicit instantiations.
template auto
eval_and_or<tenzir::ast::binary_op::and_>(evaluator&,
                                          tenzir::ast::binary_expr const&,
                                          tenzir::ActiveRows const&)
  -> array_<data>;
template auto
eval_and_or<tenzir::ast::binary_op::or_>(evaluator&,
                                         tenzir::ast::binary_expr const&,
                                         tenzir::ActiveRows const&)
  -> array_<data>;

} // namespace tenzir2
