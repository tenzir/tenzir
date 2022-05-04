//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/arrow_table_slice_builder.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/plugin.hpp>
#include <vast/transform_step.hpp>
#include <vast/type.hpp>

#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/function.h>
#include <arrow/table.h>

namespace vast::plugins::compute {

class compute_step : public transform_step {
public:
  compute_step(std::shared_ptr<arrow::compute::Function> function,
               std::unique_ptr<arrow::compute::FunctionOptions> function_options,
               std::vector<std::string> fields,
               std::vector<arrow::Datum> inputs)
    : function_{std::move(function)},
      function_options_{std::move(function_options)},
      fields_{std::move(fields)},
      inputs_{std::move(inputs)} {
    VAST_ASSERT(function_);
    VAST_ASSERT(!fields_.empty());
    VAST_ASSERT(function_->arity().is_varargs
                  ? (static_cast<size_t>(function_->arity().num_args)
                     <= 1 + inputs_.size())
                  : (static_cast<size_t>(function_->arity().num_args)
                     == 1 + inputs_.size()));
  }

  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    auto indexed_transformations = indexed_transformations_cache_.find(layout);
    if (indexed_transformations == indexed_transformations_cache_.end()) {
      // Create a new cache entry.
      indexed_transformations = indexed_transformations_cache_.emplace_hint(
        indexed_transformations_cache_.end(), std::piecewise_construct,
        std::make_tuple(layout), std::make_tuple());
      for (const auto& field : fields_) {
        for (const auto& index :
             caf::get<record_type>(layout).resolve_key_suffix(field,
                                                              layout.name())) {
          auto transformation
            = [&](struct record_type::field field,
                  std::shared_ptr<arrow::Array> array) noexcept
            -> std::vector<std::pair<struct record_type::field,
                                     std::shared_ptr<arrow::Array>>> {
            auto execute_result
              = function_->Execute({array}, function_options_.get(), nullptr);
            // FIXME: be more lenient with error handling than asserting here.
            VAST_ASSERT(execute_result.ok(),
                        execute_result.status().ToString().c_str());
            array = execute_result.MoveValueUnsafe().make_array();
            field.type = type::from_arrow(*array->type());
            return {{std::move(field), std::move(array)}};
          };
          indexed_transformations->second.push_back(
            {index, std::move(transformation)});
        }
      }
      std::sort(indexed_transformations->second.begin(),
                indexed_transformations->second.end());
    }
    // Apply the transformations.
    std::tie(layout, batch)
      = transform_columns(layout, batch, indexed_transformations->second);
    transformed_batches_.emplace_back(std::move(layout), std::move(batch));
    return caf::none;
  }

  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override {
    return std::exchange(transformed_batches_, {});
  }

private:
  /// Cache for transformed batches.
  std::vector<transform_batch> transformed_batches_ = {};

  /// Cache for the per-layout transformations.
  std::unordered_map<type, std::vector<indexed_transformation>>
    indexed_transformations_cache_ = {};

  /// The paramters of the transformation.
  std::shared_ptr<arrow::compute::Function> function_ = {};
  std::unique_ptr<arrow::compute::FunctionOptions> function_options_ = {};
  std::vector<std::string> fields_ = {};
  std::vector<arrow::Datum> inputs_ = {};
};

// -- plugin ------------------------------------------------------------------

class plugin final : public virtual transform_plugin {
public:
  caf::error initialize(data options) override {
    // We don't use any plugin-specific configuration under
    // vast.plugins.compute, so nothing is needed here.
    if (caf::holds_alternative<caf::none_t>(options))
      return caf::none;
    if (const auto* rec = caf::get_if<record>(&options))
      if (rec->empty())
        return caf::none;
    return caf::make_error(ec::invalid_configuration, "expected empty "
                                                      "configuration under "
                                                      "vast.plugins.compute");
  }

  [[nodiscard]] const char* name() const override {
    return "compute";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<transform_step>>
  make_transform_step(const record&) const override {
    // FIXME: parse function name, function options, fields, and inputs from
    // configuration.
    const auto* registry = arrow::compute::GetFunctionRegistry();
    auto get_function_result = registry->GetFunction("ascii_trim");
    if (!get_function_result.ok())
      return caf::make_error(ec::unspecified,
                             get_function_result.status().ToString());
    auto function = get_function_result.MoveValueUnsafe();
    if (function->kind() != arrow::compute::Function::SCALAR)
      return caf::make_error(ec::unimplemented, "non-scalar compute function");
    // FIXME: support non-unary functions.
    if (function->arity().num_args != 1)
      return caf::make_error(ec::unimplemented, "non-unary compute function");
    auto function_options = std::make_unique<arrow::compute::TrimOptions>(". ");
    auto fields = std::vector<std::string>{"hostname"};
    auto inputs = std::vector<arrow::Datum>{};
    return std::make_unique<compute_step>(std::move(function),
                                          std::move(function_options),
                                          std::move(fields), std::move(inputs));
    return ec::unimplemented;
  }
};

} // namespace vast::plugins::compute

VAST_REGISTER_PLUGIN(vast::plugins::compute::plugin)
