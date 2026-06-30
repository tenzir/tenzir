//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/eval_impl.hpp"
#include "tenzir/type.hpp"

#include <ranges>
#include <unordered_set>

namespace tenzir {

auto function_use::evaluator::length() const -> int64_t {
  return static_cast<tenzir::evaluator*>(self_)->length();
}

auto function_use::evaluator::get_input() const -> std::optional<table_slice> {
  auto* evaluator = static_cast<tenzir::evaluator*>(self_);
  const auto* input = evaluator->get_input();
  if (not input) {
    return std::nullopt;
  }
  return *input;
}

auto function_use::evaluator::operator()(ast::expression const& expr) const
  -> multi_series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr, {});
}

auto function_use::evaluator::operator()(
  const ast::lambda_expr& expr, const basic_series<list_type>& input) const
  -> multi_series {
  return static_cast<tenzir::evaluator*>(self_)->eval(expr, input);
}

auto function_use::evaluator::operator()(const ast::lambda_expr& expr,
                                         const basic_series<list_type>& input,
                                         int64_t input_offset) const
  -> multi_series {
  auto* evaluator = static_cast<tenzir::evaluator*>(self_);
  return evaluator->slice(input_offset, input_offset + input.length())
    .eval(expr, input);
}

auto aggregation_plugin::make_function(function_invocation inv,
                                       session ctx) const
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
  TRY(auto fn, this->make_aggregation(function_invocation{adjusted_call}, ctx));
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

namespace {

/// Verify a single operator-position argument. When a function is used as an
/// operator, its operator-facing arguments are parsed by `argument_parser2`
/// inside `make_function`, which requires compile-time constants; a non-const
/// argument would compile here but fail during per-slice evaluation and silently
/// drop events. We therefore reject any argument that is not statically
/// evaluable, and additionally type-check those that carry a declared type.
auto verify_argument(std::string_view name,
                     const std::optional<type>& value_type,
                     const ast::expression& expr, session ctx)
  -> failure_or<void> {
  auto value = try_const_eval(expr, ctx);
  if (not value) {
    diagnostic::error("argument `{}` must be a constant", name)
      .primary(expr)
      .emit(ctx);
    return failure::promise();
  }
  if (value_type and not type_check(*value_type, *value)) {
    auto actual = type::infer(*value);
    auto actual_str
      = actual ? fmt::format("{}", *actual) : std::string{"unknown"};
    diagnostic::error("argument `{}` must be of type `{}` (got `{}`)", name,
                      fmt::format("{}", *value_type), actual_str)
      .primary(expr)
      .emit(ctx);
    return failure::promise();
  }
  return {};
}

} // namespace

auto function_plugin::Signature::verify(std::span<const ast::expression> args,
                                        location op_loc, session ctx) const
  -> failure_or<void> {
  using PositionalArgument = function_plugin::PositionalArgument;
  using NamedArgument = function_plugin::NamedArgument;
  auto result = failure_or<void>{};
  auto fail = [&] {
    result = failure::promise();
  };
  // Partition the declared arguments into positional and named.
  auto positionals = std::vector<const PositionalArgument*>{};
  auto nameds = std::vector<const NamedArgument*>{};
  for (const auto& arg : arguments) {
    arg.match(
      [&](const PositionalArgument& p) {
        positionals.push_back(&p);
      },
      [&](const NamedArgument& n) {
        nameds.push_back(&n);
      });
  }
  auto seen = std::unordered_set<std::string>{};
  auto positional_idx = size_t{0};
  for (const auto& arg : args) {
    const auto* assignment = try_as<ast::assignment>(arg);
    if (assignment) {
      // Named argument: `name=value`.
      auto selector = ast::selector::try_from(assignment->left);
      const auto* sel
        = selector ? try_as<ast::field_path>(&*selector) : nullptr;
      if (not sel or sel->has_this() or sel->path().size() != 1
          or sel->path()[0].has_question_mark) {
        diagnostic::error("invalid name").primary(assignment->left).emit(ctx);
        fail();
        continue;
      }
      const auto& name = sel->path()[0].id.name;
      auto it = std::ranges::find(nameds, name, &NamedArgument::name);
      if (it == nameds.end()) {
        auto builder
          = diagnostic::error("named argument `{}` does not exist", name)
              .primary(assignment->left);
        if (not nameds.empty()) {
          const auto& best = *std::ranges::max_element(
            nameds, {}, [&](const NamedArgument* candidate) {
              return detail::calculate_similarity(name, candidate->name);
            });
          if (detail::calculate_similarity(name, best->name) > -10) {
            builder = std::move(builder).hint("did you mean `{}`?", best->name);
          }
        }
        std::move(builder).emit(ctx);
        fail();
        continue;
      }
      if (not seen.insert(name).second) {
        diagnostic::error("duplicate named argument `{}`", name)
          .primary(arg.get_location())
          .emit(ctx);
        fail();
        continue;
      }
      if (verify_argument(name, (*it)->value_type, assignment->right, ctx)
            .is_error()) {
        fail();
      }
      continue;
    }
    // Positional argument.
    if (positional_idx >= positionals.size()) {
      diagnostic::error("did not expect more positional arguments")
        .primary(arg)
        .emit(ctx);
      fail();
      continue;
    }
    const auto* positional = positionals[positional_idx];
    ++positional_idx;
    if (verify_argument(positional->name, positional->value_type, arg, ctx)
          .is_error()) {
      fail();
    }
  }
  // Report any required positional arguments that were not provided.
  for (auto i = positional_idx; i < positionals.size(); ++i) {
    if (not positionals[i]->optional) {
      diagnostic::error("expected additional positional argument `{}`",
                        positionals[i]->name)
        .primary(op_loc)
        .emit(ctx);
      fail();
    }
  }
  // Report any required named arguments that were not provided.
  for (const auto* named : nameds) {
    if (named->required and not seen.contains(named->name)) {
      diagnostic::error("required argument `{}` was not provided", named->name)
        .primary(op_loc)
        .emit(ctx);
      fail();
    }
  }
  return result;
}

} // namespace tenzir
