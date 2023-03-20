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
#include <vast/dynamic_operator.hpp>
#include <vast/error.hpp>
#include <vast/expression.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/logger.hpp>
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
class where_operator : public legacy_pipeline_operator {
public:
  /// Constructs a *where* pipeline operator.
  /// @pre *expr* must be normalized and validated
  explicit where_operator(expression expr) : expr_{std::move(expr)} {
#if VAST_ENABLE_ASSERTIONS
    const auto normalized_and_validated_expr = normalize_and_validate(expr_);
    VAST_ASSERT(normalized_and_validated_expr,
                fmt::to_string(normalized_and_validated_expr.error()).c_str());
    VAST_ASSERT(*normalized_and_validated_expr == expr_);
#endif // VAST_ENABLE_ASSERTIONS
  }

  /// Applies the transformation to a record batch with a corresponding vast
  /// schema.
  [[nodiscard]] caf::error add(table_slice slice) override {
    VAST_TRACE("where operator adds batch");
    auto tailored_expr = tailor(expr_, slice.schema());
    if (!tailored_expr) {
      transformed_.clear();
      return tailored_expr.error();
    }
    // TODO: Replace this with an Arrow-native filter function as soon as we are
    // able to directly evaluate expressions on a record batch.
    if (auto new_slice = filter(slice, *tailored_expr))
      transformed_.push_back(*new_slice);
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<table_slice>> finish() override {
    VAST_DEBUG("where operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  expression expr_ = {};

  /// The slices being transformed.
  std::vector<table_slice> transformed_ = {};
};

// Selects matching rows from the input.
class where_operator2 final
  : public schematic_operator<where_operator2, expression> {
public:
  /// Constructs a *where* pipeline operator.
  /// @pre *expr* must be normalized and validated
  explicit where_operator2(expression expr) : expr_{std::move(expr)} {
#if VAST_ENABLE_ASSERTIONS
    auto result = normalize_and_validate(expr_);
    VAST_ASSERT(result, fmt::to_string(result.error()).c_str());
    VAST_ASSERT(*result == expr_);
#endif // VAST_ENABLE_ASSERTIONS
  }

  auto initialize(const type& schema) const -> caf::expected<State> override {
    return tailor(expr_, schema);
  }

  auto process(table_slice slice, State& expr) const -> Output override {
    // TODO: Adjust filter function return type.
    // TODO: Replace this with an Arrow-native filter function as soon as we
    // are able to directly evaluate expressions on a record batch.
    return filter(slice, expr).value_or(table_slice{});
  }

  auto to_string() const -> std::string override {
    return fmt::format("where {}", expr_);
  };

private:
  expression expr_;
};

class plugin final : public virtual pipeline_operator_plugin,
                     public virtual operator_plugin {
public:
  [[nodiscard]] caf::error
  initialize([[maybe_unused]] const record& plugin_config,
             [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "where";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<legacy_pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    auto config = to<configuration>(options);
    if (!config)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("where pipeline operator failed to "
                                         "parse "
                                         "configuration {}: {}",
                                         options, config.error()));
    auto expr = to<expression>(config->expression);
    if (!expr)
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("where pipeline operator failed to "
                                         "parse "
                                         "expression '{}': {}",
                                         config->expression, expr.error()));
    auto normalized_and_validated_expr
      = normalize_and_validate(std::move(*expr));
    if (!normalized_and_validated_expr)
      return caf::make_error(
        ec::invalid_configuration,
        fmt::format("where pipeline operator failed to normalized and validate "
                    "expression '{}': {}",
                    *expr, normalized_and_validated_expr.error()));
    return std::make_unique<where_operator>(
      std::move(*normalized_and_validated_expr));
  }

  [[nodiscard]] std::pair<
    std::string_view, caf::expected<std::unique_ptr<legacy_pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
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
      std::make_unique<where_operator2>(
        std::move(*normalized_and_validated_expr)),
    };
  }
};

} // namespace

} // namespace vast::plugins::where

VAST_REGISTER_PLUGIN(vast::plugins::where::plugin)
