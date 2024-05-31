//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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
#include <tenzir/select_optimization.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql/basic.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

#include <optional>

namespace tenzir::plugins::where {

namespace {

/// The configuration of the *where* pipeline operator.
struct configuration {
  // The expression in the config file.
  std::string expression;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f.apply(x.expression);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"expression", string_type{}},
    };
    return result;
  }
};

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

  auto optimize(expression const& filter, event_order order,
                select_optimization const& selection) const
    -> optimize_result override {
    (void)selection;
    if (filter == trivially_true_expression()) {
      return optimize_result{expr_.inner, order, nullptr, std::nullopt};
    }
    auto combined = normalize_and_validate(conjunction{expr_.inner, filter});
    TENZIR_ASSERT(combined);
    return optimize_result{std::move(*combined), order, nullptr, std::nullopt};
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

class plugin final : public virtual operator_plugin<where_operator> {
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

} // namespace
} // namespace tenzir::plugins::where

TENZIR_REGISTER_PLUGIN(tenzir::plugins::where::plugin)
