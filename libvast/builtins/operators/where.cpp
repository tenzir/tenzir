//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/expression.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>

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
  /// Constructs a *where* pipeline operator.
  /// @pre *expr* must be normalized and validated
  explicit where_operator(expression expr) : expr_{std::move(expr)} {
#if VAST_ENABLE_ASSERTIONS
    auto result = normalize_and_validate(expr_);
    VAST_ASSERT(result, fmt::to_string(result.error()).c_str());
    VAST_ASSERT(*result == expr_);
#endif // VAST_ENABLE_ASSERTIONS
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    auto tailored_expr = tailor(expr_, schema);
    // Failing to tailor in this context is not an error, it just means that we
    // cannot do anything with the result.
    if (not tailored_expr)
      return std::nullopt;
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
    return std::pair{conjunction{expr_, expr}, nullptr};
  }

  auto to_string() const -> std::string override {
    return fmt::format("where {}", expr_);
  };

private:
  expression expr_;
};

class plugin final : public virtual operator_plugin {
public:
  [[nodiscard]] caf::error
  initialize([[maybe_unused]] const record& plugin_config,
             [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "where";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::expr;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> expr >> optional_ws_or_comment
                   >> end_of_pipeline_operator;
    auto parse_result = expression{};
    if (!p(f, l, parse_result)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse where "
                                                      "operator: '{}'",
                                                      pipeline)),
      };
    }
    auto normalized_and_validated_expr = normalize_and_validate(parse_result);
    if (!normalized_and_validated_expr) {
      return {
        std::string_view{f, l},
        caf::make_error(
          ec::invalid_configuration,
          fmt::format("failed to normalized and validate expression '{}': {}",
                      parse_result, normalized_and_validated_expr.error())),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<where_operator>(
        std::move(*normalized_and_validated_expr)),
    };
  }
};

} // namespace

} // namespace vast::plugins::where

VAST_REGISTER_PLUGIN(vast::plugins::where::plugin)
