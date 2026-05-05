//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"
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
  static auto
  eval(arrow::NullArray const& x, auto warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::NullArray> {
    TENZIR_UNUSED(warn);
    TENZIR_UNUSED(active);
    return std::make_shared<arrow::NullArray>(x.data());
  }
};

template <>
struct EvalUnOp<ast::unary_op::not_, bool_type> {
  static auto
  eval(arrow::BooleanArray const& x, auto warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::BooleanArray> {
    TENZIR_UNUSED(warn);
    TENZIR_UNUSED(active);
    const auto& input = x.values();
    auto output = check(arrow::AllocateBuffer(input->size()));
    auto* input_ptr = input->data();
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

  static auto
  eval(type_to_arrow_array_t<T> const& x, auto warn, ActiveRows const& active)
    -> std::shared_ptr<type_to_arrow_array_t<U>> {
    auto b = type_to_arrow_builder_t<U>{tenzir::arrow_memory_pool()};
    check(b.Reserve(x.length()));
    auto overflow = false;
    for (auto i = int64_t{0}; i < x.length(); ++i) {
      if (not active.is_active(i) or x.IsNull(i)) {
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
  static auto
  eval(arrow::DurationArray const& x, auto warn, ActiveRows const& active)
    -> std::shared_ptr<arrow::DurationArray> {
    auto b = duration_type::make_arrow_builder(arrow_memory_pool());
    check(b->Reserve(x.length()));
    auto overflow = false;
    for (auto i = int64_t{0}; i < x.length(); ++i) {
      if (not active.is_active(i) or x.IsNull(i)) {
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

auto evaluator::eval(ast::unary_expr const& x, ActiveRows const& active)
  -> multi_series {
  auto eval_op = [&]<ast::unary_op Op>() -> multi_series {
    TENZIR_ASSERT(x.op.inner == Op);
    auto offset = int64_t{0};
    return map_series(eval(x.expr, active), [&](series v) {
      auto active_slice = active.slice(offset, v.length());
      offset += v.length();
      return match(v.type, [&]<concrete_type T>(const T&) -> series {
        if constexpr (caf::detail::is_complete<EvalUnOp<Op, T>>) {
          auto& a = as<type_to_arrow_array_t<T>>(*v.array);
          auto oa = EvalUnOp<Op, T>::eval(
            a,
            [&](std::string_view msg) {
              diagnostic::warning("{}", msg).primary(x).emit(ctx_);
            },
            active_slice);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          if (active_slice.as_constant() != false) {
            diagnostic::warning("unary operator `{}` not implemented for `{}`",
                                x.op.inner, v.type.kind())
              .primary(x)
              .emit(ctx_);
          }
          return series::null(null_type{}, v.length());
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
    case move:
    case take: {
      auto keyword = x.op.inner == move ? "move" : "take";
      if (ast::field_path::try_from(x.expr)) {
        diagnostic::warning("{} is not supported here", keyword)
          .primary(x.op, "has no effect")
          .hint("{} only works on fields within assignments", keyword)
          .emit(ctx_);
      } else {
        diagnostic::warning("{} has no effect", keyword)
          .primary(x.expr, "is not a field")
          .hint("{} only works on fields within assignments", keyword)
          .emit(ctx_);
      }
      return eval(x.expr, active);
    }
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
