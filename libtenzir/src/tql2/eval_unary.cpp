//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/tql2/eval_impl.hpp"
#include "tenzir/type.hpp"

#include <string_view>

namespace tenzir {

namespace {

template <ast::unary_op Op, concrete_type T>
struct EvalUnOp;

template <ast::unary_op Op>
struct EvalUnOp<Op, null_type> {
  static auto eval(const arrow::NullArray& x, auto warn)
    -> std::shared_ptr<arrow::NullArray> {
    TENZIR_UNUSED(warn);
    return std::make_shared<arrow::NullArray>(x.data());
  }
};

template <>
struct EvalUnOp<ast::unary_op::not_, bool_type> {
  static auto eval(const arrow::BooleanArray& x, auto warn)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO: Make sure this works or use simpler version.
    TENZIR_UNUSED(warn);
    const auto& input = x.values();
    auto output = check(arrow::AllocateBuffer(input->size()));
    const auto* input_ptr = input->data();
    auto* output_ptr = output->mutable_data();
    auto length = detail::narrow<size_t>(input->size());
    for (auto i = size_t{0}; i < length; ++i) {
      output_ptr[i] = ~input_ptr[i]; // NOLINT
    }
    return std::make_shared<arrow::BooleanArray>(x.length(), std::move(output),
                                                 x.null_bitmap(),
                                                 x.data()->null_count,
                                                 x.offset());
  }
};

template <numeric_type T>
struct EvalUnOp<ast::unary_op::neg, T> {
  using U = std::conditional_t<std::same_as<T, uint64_type>, int64_type, T>;

  static auto eval(const type_to_arrow_array_t<T>& x, auto warn)
    -> std::shared_ptr<type_to_arrow_array_t<U>> {
    auto b = type_to_arrow_builder_t<U>{};
    check(b.Reserve(x.length()));
    auto overflow = false;
    for (auto i = int64_t{0}; i < x.length(); ++i) {
      if (x.IsNull(i)) {
        check(b.AppendNull());
        continue;
      }
      auto val = x.Value(i);
      if constexpr (std::same_as<T, int64_type>) {
        if (val == std::numeric_limits<int64_t>::min()) {
          overflow = true;
          check(b.AppendNull());
          continue;
        }
        check(b.Append(-val));
      } else if constexpr (std::same_as<T, uint64_type>) {
        if (val
            > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1) {
          overflow = true;
          check(b.AppendNull());
          continue;
        }
        check(b.Append(-static_cast<int64_t>(val)));
      } else {
        static_assert(std::same_as<T, double_type>);
        check(b.Append(-val));
      }
    }
    if (overflow) {
      warn("integer overflow");
    }
    return finish(b);
  }
};

template <>
struct EvalUnOp<ast::unary_op::neg, duration_type> {
  static auto eval(const arrow::DurationArray& x, auto warn)
    -> std::shared_ptr<arrow::DurationArray> {
    auto b = duration_type::make_arrow_builder(arrow::default_memory_pool());
    check(b->Reserve(x.length()));
    auto overflow = false;
    for (auto i = int64_t{0}; i < x.length(); ++i) {
      if (x.IsNull(i)) {
        check(b->AppendNull());
        continue;
      }
      auto val = x.Value(i);
      static_assert(std::same_as<decltype(val), duration::rep>);
      if (val == std::numeric_limits<duration::rep>::min()) {
        overflow = true;
        check(b->AppendNull());
        continue;
      }
      check(b->Append(-val));
    }
    if (overflow) {
      warn("duration negation overflow");
    }
    return finish(*b);
  }
};

} // namespace

auto evaluator::eval(const ast::unary_expr& x) -> multi_series {
  auto eval_op = [&]<ast::unary_op Op>() -> multi_series {
    TENZIR_ASSERT(x.op.inner == Op);
    return map_series(eval(x.expr), [&](series v) {
      return match(v.type, [&]<concrete_type T>(const T&) -> series {
        if constexpr (caf::detail::is_complete<EvalUnOp<Op, T>>) {
          auto& a = as<type_to_arrow_array_t<T>>(*v.array);
          auto oa = EvalUnOp<Op, T>::eval(a, [&](std::string_view msg) {
            diagnostic::warning("{}", msg).primary(x).emit(ctx_);
          });
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          diagnostic::warning("unary operator `{}` not implemented for `{}`",
                              x.op.inner, v.type.kind())
            .primary(x)
            .emit(ctx_);
          return null();
        }
      });
    });
  };
  using enum ast::unary_op;
  switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval_op.operator()<op>()
    X(pos);
    X(neg);
    X(not_);
#undef X
    case move: {
      if (ast::field_path::try_from(x.expr)) {
        diagnostic::warning("move is not supported here")
          .primary(x.op, "has no effect")
          .hint("move only works on fields within assignments")
          .emit(ctx_);
      } else {
        diagnostic::warning("move has no effect")
          .primary(x.expr, "is not a field")
          .hint("move only works on fields within assignments")
          .emit(ctx_);
      }
      return eval(x.expr);
    }
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
