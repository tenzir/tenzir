//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/error.hpp>
#include <vast/expression.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder_factory.hpp>

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
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"expression", string_type{}},
    };
    return result;
  }
};

// Selects matching rows from the input.
class where_operator : public pipeline_operator {
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
  /// layout.
  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_TRACE("where operator adds batch");
    auto tailored_expr = tailor(expr_, layout);
    if (!tailored_expr) {
      transformed_.clear();
      return tailored_expr.error();
    }
    // TODO: Replace this with an Arrow-native filter function as soon as we are
    // able to directly evaluate expressions on a record batch.
    if (auto new_slice = filter(table_slice{batch}, *tailored_expr)) {
      auto as_batch = to_record_batch(*new_slice);
      transformed_.emplace_back(new_slice->layout(), std::move(as_batch));
    }
    return caf::none;
  }

  /// Retrieves the result of the transformation.
  [[nodiscard]] caf::expected<std::vector<pipeline_batch>> finish() override {
    VAST_DEBUG("where operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  expression expr_ = {};

  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_ = {};
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  [[nodiscard]] caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "where";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
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
};

} // namespace

} // namespace vast::plugins::where

VAST_REGISTER_PLUGIN(vast::plugins::where::plugin)
