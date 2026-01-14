//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval_impl.hpp"

namespace tenzir {

auto function_use::evaluator::length() const -> int64_t {
  return static_cast<tenzir::evaluator*>(self_)->length();
}

auto function_use::evaluator::operator()(const ast::expression& expr) const
  -> multi_series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr);
}

auto function_use::evaluator::operator()(
  const ast::lambda_expr& expr, const basic_series<list_type>& input) const
  -> multi_series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr, input);
}

auto aggregation_plugin::make_function(invocation inv, session ctx) const
  -> failure_or<function_ptr> {
  if (inv.call.args.empty()) {
    diagnostic::error("aggregation functions need at least one list argument "
                      "to be called as regular functions")
      .hint("use with `summarize` to aggregate over multiple events instead")
      .primary(inv.call)
      .emit(ctx);
    return failure::promise();
  }
  auto subject_arg = inv.call.args.front();
  auto adjusted_call = inv.call;
  auto inner_selector
    = ast::root_field{ast::identifier{"x", subject_arg.get_location()}};
  adjusted_call.args.front() = inner_selector;
  TRY(auto fn, this->make_aggregation(invocation{adjusted_call}, ctx));
  return function_use::make(
    [fn = std::move(fn), subject_arg = std::move(subject_arg)](
      evaluator eval, session ctx) mutable -> multi_series {
      return map_series(eval(subject_arg), [&](series subject) -> series {
        if (is<null_type>(subject.type)) {
          return series::null(null_type{}, subject.length());
        }
        const auto lists = subject.as<list_type>();
        if (not lists) {
          diagnostic::warning("expected `list`, but got `{}`",
                              subject.type.kind())
            .primary(subject_arg)
            .emit(ctx);
          return series::null(null_type{}, subject.length());
        }
        const auto dummy_type = type{
          "dummy",
          record_type{
            {"x", lists->type.value_type()},
          },
        };
        auto slice = table_slice{
          arrow::RecordBatch::Make(dummy_type.to_arrow_schema(),
                                   lists->array->values()->length(),
                                   arrow::ArrayVector{lists->array->values()}),
          dummy_type,
        };
        auto builder = series_builder{};
        for (auto i = int64_t{}; i < lists->array->length(); ++i) {
          if (lists->array->IsNull(i)) {
            builder.null();
            continue;
          }
          const auto start = lists->array->value_offset(i);
          const auto end = start + lists->array->value_length(i);
          if (start != end) {
            fn->update(subslice(slice, start, end), ctx);
          }
          builder.data(fn->get());
          fn->reset();
        }
        return builder.finish_assert_one_array();
      });
    });
}

auto function_use::make(
  detail::unique_function<auto(evaluator eval, session ctx)->multi_series> f)
  -> std::unique_ptr<function_use> {
  class result final : public function_use {
  public:
    explicit result(
      detail::unique_function<auto(evaluator eval, session ctx)->multi_series> f)
      : f_{std::move(f)} {
    }

    auto run(evaluator eval, session ctx) -> multi_series override {
      return f_(eval, ctx);
    }

  private:
    detail::unique_function<auto(evaluator eval, session ctx)->multi_series> f_;
  };
  return std::make_unique<result>(std::move(f));
}

auto function_plugin::function_name() const -> std::string {
  // TODO: Remove this once we removed TQL1 plugins.
  auto result = name();
  if (result.starts_with("tql2.")) {
    result.erase(0, 5);
  }
  return result;
}

} // namespace tenzir
