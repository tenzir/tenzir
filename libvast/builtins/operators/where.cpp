//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/diagnostics.hpp>
#include <vast/error.hpp>
#include <vast/expression.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/tql/basic.hpp>

#include <arrow/type.h>
#include <caf/expected.hpp>

namespace vast::plugins::where {

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
#if VAST_ENABLE_ASSERTIONS
    auto result = normalize_and_validate(expr_.inner);
    VAST_ASSERT(result, fmt::to_string(result.error()).c_str());
    VAST_ASSERT(*result == expr_.inner, fmt::to_string(result).c_str());
#endif // VAST_ENABLE_ASSERTIONS
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto tailored_expr = tailor(expr_.inner, schema);
    // Failing to tailor in this context is not an error, it just means that we
    // cannot do anything with the result.
    if (not tailored_expr) {
      diagnostic::warning("{}", tailored_expr.error())
        .primary(expr_.source)
        .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    return std::move(*tailored_expr);
  }

  auto process(table_slice slice, state_type& expr) const
    -> output_type override {
    // TODO: Adjust filter function return type.
    // TODO: Replace this with an Arrow-native filter function as soon as we
    // are able to directly evaluate expressions on a record batch.
    if (expr)
      return filter(slice, *expr).value_or(table_slice{});
    return {};
  }

  auto predicate_pushdown(expression const& expr) const
    -> std::optional<std::pair<expression, operator_ptr>> override {
    if (expr == trivially_true_expression()) {
      return std::pair{expr_.inner, nullptr};
    }
    auto expr_conjunction = conjunction{expr_.inner, expr};
    auto result = normalize_and_validate(expr_conjunction);
    VAST_ASSERT(result);
    return std::pair{std::move(*result), nullptr};
  }

  auto to_string() const -> std::string override {
    return fmt::format("where {}", expr_.inner);
  };

  auto name() const -> std::string override {
    return "where";
  }

  friend auto inspect(auto& f, where_operator& x) -> bool {
    return f.apply(x.expr_);
  }

private:
  located<expression> expr_;
};

class plugin final : public virtual operator_plugin<where_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"where", "https://docs.tenzir.com/next/"
                                           "operators/transformations/where"};
    auto expr = located<vast::expression>{};
    parser.add(expr, "<expr>");
    parser.parse(p);
    auto normalized_and_validated = normalize_and_validate(expr.inner);
    if (!normalized_and_validated) {
      diagnostic::error("invalid expression")
        .primary(expr.source)
        .docs("https://tenzir.com/docs/expressions")
        .throw_();
    }
    expr.inner = std::move(*normalized_and_validated);
    return std::make_unique<where_operator>(std::move(expr));
  }
};

} // namespace
} // namespace vast::plugins::where

VAST_REGISTER_PLUGIN(vast::plugins::where::plugin)
