//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/arrow_utils.hpp"
#include "tenzir/tql2/eval_impl.hpp"

namespace tenzir {

namespace {

template <ast::unary_op Op, concrete_type T>
struct EvalUnOp;

template <>
struct EvalUnOp<ast::unary_op::not_, bool_type> {
  static auto eval(const arrow::BooleanArray& x)
    -> std::shared_ptr<arrow::BooleanArray> {
    // TODO
    auto input = x.values();
    auto output = check(arrow::AllocateBuffer(input->size()));
    auto input_ptr = input->data();
    auto output_ptr = output->mutable_data();
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

} // namespace

auto evaluator::eval(const ast::unary_expr& x) -> series {
  auto v = eval(x.expr);
  auto eval_op =
    [&]<ast::unary_op Op>(std::integral_constant<ast::unary_op, Op>) -> series {
    // auto v = to_series(eval(x.expr));
    TENZIR_ASSERT(x.op.inner == Op);
    return caf::visit(
      [&]<concrete_type T>(const T&) -> series {
        if constexpr (caf::detail::is_complete<EvalUnOp<Op, T>>) {
          auto& a = caf::get<type_to_arrow_array_t<T>>(*v.array);
          auto oa = EvalUnOp<Op, T>::eval(a);
          auto ot = type::from_arrow(*oa->type());
          return series{std::move(ot), std::move(oa)};
        } else {
          diagnostic::warning("unary operator `{}` not implemented for `{}`",
                              x.op.inner, v.type.kind())
            .primary(x.get_location())
            .emit(dh_);
          return null();
        }
      },
      v.type);
  };
  using enum ast::unary_op;
  switch (x.op.inner) {
#define X(op)                                                                  \
  case op:                                                                     \
    return eval_op(std::integral_constant<ast::unary_op, op>{})
    X(pos);
    X(neg);
    X(not_);
#undef X
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
