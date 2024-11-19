//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/debug_writer.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/error.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql/basic.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/try.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api.h>
#include <arrow/type.h>
#include <caf/expected.hpp>

namespace tenzir::plugins::where {

namespace {

// Selects matching rows from the input.
class where_operator final
  : public schematic_operator<where_operator, std::optional<expression>> {
public:
  where_operator() = default;

  /// Constructs a *where* pipeline operator.
  /// @pre *expr* must be normalized and validated
  explicit where_operator(located<expression> expr) : expr_{std::move(expr)} {
#if TENZIR_ENABLE_ASSERTIONS
    auto result = normalize_and_validate(expr_.inner);
    TENZIR_ASSERT(result, fmt::to_string(result.error()).c_str());
    TENZIR_ASSERT(*result == expr_.inner, fmt::to_string(result).c_str());
#endif // TENZIR_ENABLE_ASSERTIONS
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto ts = taxonomies{.concepts = modules::concepts()};
    auto resolved_expr = resolve(ts, expr_.inner, schema);
    if (not resolved_expr) {
      diagnostic::warning(resolved_expr.error())
        .primary(expr_.source)
        .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    auto tailored_expr = tailor(std::move(*resolved_expr), schema);
    // We ideally want to warn when extractors can not be resolved. However,
    // this is tricky for e.g. `where #schema == "foo" && bar == 42` and
    // changing the behavior for this is tricky with the current expressions.
    if (not tailored_expr) {
      // diagnostic::warning(tailored_expr.error())
      //   .primary(expr_.source)
      //   .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    return std::move(*tailored_expr);
  }

  auto process(table_slice slice, state_type& expr) const
    -> output_type override {
    // TODO: Adjust filter function return type.
    // TODO: Replace this with an Arrow-native filter function as soon as we
    // are able to directly evaluate expressions on a record batch.
    if (expr) {
      return filter(slice, *expr).value_or(table_slice{});
    }
    return {};
  }

  auto name() const -> std::string override {
    return "where";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if (filter == trivially_true_expression()) {
      return optimize_result{expr_.inner, order, nullptr};
    }
    auto combined = normalize_and_validate(conjunction{expr_.inner, filter});
    TENZIR_ASSERT(combined);
    return optimize_result{std::move(*combined), order, nullptr};
  }

  friend auto inspect(auto& f, where_operator& x) -> bool {
    if (auto dbg = as_debug_writer(f)) {
      return dbg->fmt_value("({} @ {:?})", x.expr_.inner, x.expr_.source);
    }
    return f.apply(x.expr_);
  }

private:
  located<expression> expr_;
};

class tql1_plugin final : public virtual operator_plugin<where_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"where", "https://docs.tenzir.com/"
                                           "operators/where"};
    auto expr = located<tenzir::expression>{};
    parser.add(expr, "<expr>");
    parser.parse(p);
    auto normalized_and_validated = normalize_and_validate(expr.inner);
    if (!normalized_and_validated) {
      diagnostic::error("invalid expression")
        .primary(expr.source)
        .docs("https://tenzir.com/language/expressions")
        .throw_();
    }
    expr.inner = std::move(*normalized_and_validated);
    return std::make_unique<where_operator>(std::move(expr));
  }
};

class tql2_where_assert_operator final
  : public crtp_operator<tql2_where_assert_operator> {
public:
  tql2_where_assert_operator() = default;

  explicit tql2_where_assert_operator(ast::expression expr, bool warn)
    : expr_{std::move(expr)}, warn_{warn} {
  }

  auto name() const -> std::string override {
    return warn_ ? "tql2.assert" : "tql2.where";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: This might be quite inefficient compared to what we could do.
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto filter = eval(expr_, slice, ctrl.diagnostics());
      auto array = try_as<arrow::BooleanArray>(&*filter.array);
      if (not array) {
        diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
          .primary(expr_)
          .emit(ctrl.diagnostics());
        co_yield {};
        continue;
      }
      if (array->true_count() == array->length()) {
        co_yield std::move(slice);
        continue;
      }
      if (warn_) {
        diagnostic::warning("assertion failure")
          .primary(expr_)
          .emit(ctrl.diagnostics());
      }
      auto length = array->length();
      auto current_value = array->Value(0);
      auto current_begin = int64_t{0};
      // We add an artificial `false` at index `length` to flush.
      auto results = std::vector<table_slice>{};
      for (auto i = int64_t{1}; i < length + 1; ++i) {
        const auto next = i != length && array->IsValid(i) && array->Value(i);
        if (current_value == next) {
          continue;
        }
        if (current_value) {
          results.push_back(subslice(slice, current_begin, i));
        }
        current_value = next;
        current_begin = i;
      }
      co_yield concatenate(std::move(results));
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    if (warn_) {
      return optimize_result::order_invariant(*this, order);
    }
    auto [legacy, remainder] = split_legacy_expression(expr_);
    auto remainder_op = is_true_literal(remainder)
                          ? nullptr
                          : std::make_unique<tql2_where_assert_operator>(
                            std::move(remainder), warn_);
    if (filter == trivially_true_expression()) {
      return optimize_result{std::move(legacy), order, std::move(remainder_op)};
    }
    auto combined
      = normalize_and_validate(conjunction{std::move(legacy), filter});
    TENZIR_ASSERT(combined);
    return optimize_result{std::move(*combined), order,
                           std::move(remainder_op)};
  }

  friend auto inspect(auto& f, tql2_where_assert_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("warn", x.warn_));
  }

private:
  ast::expression expr_;
  bool warn_;
};

TENZIR_ENUM(mode, map, where);

struct arguments {
  ast::expression field;
  ast::simple_selector capture;
  ast::expression expr;
};

auto make_where_map_function(function_plugin::invocation inv, session ctx,
                             enum mode mode) -> failure_or<function_ptr> {
  auto args = arguments{};
  TRY(argument_parser2::function(fmt::to_string(mode))
        .add(args.field, "<field>")
        .add(args.capture, "<capture>")
        .add(args.expr, "<expr>")
        .parse(inv, ctx));
  // We internally use the empty string for our top-level dummy field, so it
  // must not be used in the capture name.
  if (args.capture.has_this() and not args.capture.path().empty()
      and args.capture.path().front().name.empty()) {
    diagnostic::error("capture name must not start with an empty string")
      .primary(args.capture.path().front().location)
      .emit(ctx);
    return failure::promise();
  }
  return function_use::make(
    [mode, args = std::move(args)](function_plugin::evaluator eval,
                                   session ctx) -> series {
      auto field = eval(args.field);
      if (field.as<null_type>()) {
        return field;
      }
      auto field_list = field.as<list_type>();
      if (not field_list) {
        diagnostic::error("expected `list`, but got `{}`", field.type.kind())
          .primary(args.field)
          .emit(ctx);
        return series::null(null_type{}, eval.length());
      }
      // We get the schema name from the parent evsaluator so that we can make
      // @name available in the mapped expression.
      const auto name
        = eval(ast::meta{ast::meta::name, location::unknown}).as<string_type>();
      TENZIR_ASSERT(name);
      TENZIR_ASSERT(name->length() > 0);
      TENZIR_ASSERT(name->array->IsValid(0));
      // We get the field's inner values array and create a dummy table slice
      // with a single field to evaluate the mapped expression on.
      auto values
        = series{field_list->type.value_type(), field_list->array->values()};
      auto slice = table_slice{
        check(arrow::RecordBatch::FromStructArray(
          std::make_shared<arrow::StructArray>(
            arrow::struct_({{"", arrow::null()}}), values.length(),
            std::vector{
              check(arrow::MakeArrayOfNull(arrow::null(), values.length()))}))),
        type{name->array->GetView(0), record_type{{"", null_type{}}}},
      };
      slice = assign(args.capture, values, slice, ctx);
      TENZIR_ASSERT(as<record_type>(slice.schema()).num_fields() == 2);
      slice = transform_columns(
        slice, {{offset{0},
                 [](struct record_type::field, std::shared_ptr<arrow::Array>) {
                   return indexed_transformation::result_type{};
                 }}});
      values = tenzir::eval(args.expr, slice, ctx);
      switch (mode) {
        case mode::map: {
          // Lastly, we create a new series with the value offsets from the
          // original list array and the mapped list array's values.
          return series{
            list_type{values.type},
            std::make_shared<arrow::ListArray>(
              arrow::list(values.array->type()), field_list->array->length(),
              field_list->array->value_offsets(), values.array,
              field_list->array->null_bitmap(), field_list->array->null_count(),
              field_list->array->offset()),
          };
        }
        case mode::where: {
          const auto predicate = values.as<bool_type>();
          if (not predicate) {
            diagnostic::warning("expected `bool`, but got `{}`",
                                values.type.kind())
              .primary(args.expr)
              .emit(ctx);
            return series::null(field.type, field.length());
          }
          if (predicate->array->null_count() != 0) {
            diagnostic::warning("expected `bool`, got `null`")
              .primary(args.expr)
              .emit(ctx);
          } else if (predicate->array->false_count() == 0) {
            return field;
          }
          auto predicate_gen = predicate->values();
          auto builder = series_builder{field.type};
          match(field_list->type.value_type(), [&]<concrete_type T>(const T&) {
            for (auto&& list : field_list->values()) {
              if (not list) {
                builder.null();
                continue;
              }
              auto list_builder = builder.list();
              for (auto&& element : *list) {
                auto should_filter = predicate_gen.next();
                TENZIR_ASSERT(should_filter);
                if (should_filter->value_or(false)) {
                  list_builder.data(as<view<type_to_data_t<T>>>(element));
                }
              }
            }
            // Check that we actually did iterate over all evaluated
            TENZIR_ASSERT(not predicate_gen.next());
          });
          return builder.finish_assert_one_array();
        }
      }
      TENZIR_UNREACHABLE();
    });
}

class assert_plugin final
  : public virtual operator_plugin2<tql2_where_assert_operator> {
public:
  auto name() const -> std::string override {
    return "tql2.assert";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::operator_("assert").add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<tql2_where_assert_operator>(std::move(expr), true);
  }
};

class where_plugin final : public virtual operator_factory_plugin,
                           public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.where";
  }

  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto expr = ast::expression{};
    TRY(
      argument_parser2::operator_("where").add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<tql2_where_assert_operator>(std::move(expr), false);
  }

  auto make_function(function_plugin::invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return make_where_map_function(std::move(inv), ctx, mode::where);
  }
};

class map_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.map";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    return make_where_map_function(std::move(inv), ctx, mode::map);
  }
};

} // namespace
} // namespace tenzir::plugins::where

TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::tql1_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::assert_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::where_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::map_plugin)
