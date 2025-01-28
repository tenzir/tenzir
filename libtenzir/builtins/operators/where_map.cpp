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

#include <ranges>

namespace tenzir::plugins::where {

TENZIR_ENUM(mode, map, where);

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

class where_assert_operator final
  : public crtp_operator<where_assert_operator> {
public:
  where_assert_operator() = default;

  explicit where_assert_operator(ast::expression expr, bool warn)
    : expr_{std::move(expr)}, warn_{warn} {
  }

  auto name() const -> std::string override {
    return "where_assert_operator";
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
      auto offset = int64_t{0};
      for (auto& filter : eval(expr_, slice, ctrl.diagnostics())) {
        auto array = try_as<arrow::BooleanArray>(&*filter.array);
        if (not array) {
          diagnostic::warning("expected `bool`, got `{}`", filter.type.kind())
            .primary(expr_)
            .emit(ctrl.diagnostics());
          offset += filter.array->length();
          co_yield {};
          continue;
        }
        if (array->true_count() == array->length()) {
          co_yield subslice(slice, offset, offset + array->length());
          offset += array->length();
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
            results.push_back(
              subslice(slice, offset + current_begin, offset + i));
          }
          current_value = next;
          current_begin = i;
        }
        co_yield concatenate(std::move(results));
        offset += length;
      }
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
                          : std::make_unique<where_assert_operator>(
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

  friend auto inspect(auto& f, where_assert_operator& x) -> bool {
    return f.object(x).fields(f.field("expression", x.expr_),
                              f.field("warn", x.warn_));
  }

private:
  ast::expression expr_;
  bool warn_;
};

struct arguments {
  ast::expression field;
  ast::simple_selector capture;
  ast::expression expr;
};

auto make_where_map_function(function_plugin::invocation inv, session ctx,
                             enum mode mode) -> failure_or<function_ptr> {
  auto args = arguments{};
  TRY(argument_parser2::function(fmt::to_string(mode))
        .positional("list", args.field, "list")
        .positional("capture", args.capture)
        .positional("expression", args.expr, "any")
        .parse(inv, ctx));
  return function_use::make([mode, args = std::move(args)](
                              function_plugin::evaluator eval, session ctx) {
    return map_series(eval(args.field), [&](series field) -> multi_series {
      if (field.as<null_type>()) {
        return field;
      }
      auto field_list = field.as<list_type>();
      if (not field_list) {
        diagnostic::warning("expected `list`, but got `{}`", field.type.kind())
          .primary(args.field)
          .emit(ctx);
        return series::null(null_type{}, eval.length());
      }
      // We get the field's inner values array and create a dummy table slice
      // with a single field to evaluate the mapped expression on. TODO: We
      // should consider unrolling the surrounding event to make more than just
      // the capture available. This may be rather expensive, though, so we
      // should consider doing some static analysis to only unroll the fields
      // actually used.
      auto list_values
        = series{field_list->type.value_type(), field_list->array->values()};
      if (list_values.length() == 0) {
        return field;
      }
      // TODO: The name here is somewhat arbitrary. It could be accessed if
      // `@name` where to be used inside the inner expression.
      const auto empty_type = type{fmt::to_string(mode), record_type{}};
      auto slice = table_slice{
        arrow::RecordBatch::Make(empty_type.to_arrow_schema(),
                                 list_values.length(), arrow::ArrayVector{}),
        empty_type,
      };
      slice = assign(args.capture, list_values, slice, ctx);
      auto ms = tenzir::eval(args.expr, slice, ctx);
      TENZIR_ASSERT(not ms.parts().empty());
      /// TODO: Should the conflict resolution be exposed to the user?
      auto [values, result, conflicts] = ms.to_series(
        multi_series::to_series_strategy::take_largest_null_rest);
      if (result != multi_series::to_series_result::status_t::ok) {
        // TODO: The error message is bad. It's difficult to explain.
        auto kinds = std::set<type_kind>{};
        for (const auto& c : conflicts) {
          kinds.insert(c.kind());
        }
        diagnostic::warning("expression evaluated to incompatible types")
          .primary(args.expr, "types `{}` are incompatible",
                   fmt::join(kinds, "`, `"))
          .emit(ctx);
        if (result == multi_series::to_series_result::status_t::fail) {
          return series::null(null_type{}, ms.length());
        }
      }
      switch (mode) {
        case mode::map: {
          // Lastly, we create a new series with the value offsets from the
          // original list array and the mapped list array's values.
          return series{
            list_type{values.type},
            std::make_shared<arrow::ListArray>(
              list_type{values.type}.to_arrow_type(),
              field_list->array->length(), field_list->array->value_offsets(),
              values.array, field_list->array->null_bitmap(),
              field_list->array->null_count(), field_list->array->offset()),
          };
        }
        case mode::where: {
          if (values.as<null_type>()) {
            auto builder = series_builder{field.type};
            for (auto i = int64_t{0}; i < field.length(); ++i) {
              builder.list();
            }
            return builder.finish_assert_one_array();
          }
          const auto predicate = values.as<bool_type>();
          if (not predicate) {
            diagnostic::warning("expected `bool`, but got `{}`",
                                values.type.kind())
              .primary(args.expr)
              .emit(ctx);
            return series::null(field.type, field.length());
          }
          if (predicate->array->true_count() == predicate->length()) {
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
  });
}

using where_assert_plugin = operator_inspection_plugin<where_assert_operator>;

class assert_plugin final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.assert";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::operator_("assert")
          .positional("invariant", expr, "bool")
          .parse(inv, ctx));
    return std::make_unique<where_assert_operator>(std::move(expr), true);
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
    TRY(argument_parser2::operator_("where")
          .positional("predicate", expr, "bool")
          .parse(inv, ctx));
    return std::make_unique<where_assert_operator>(std::move(expr), false);
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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::where_assert_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::map_plugin)
