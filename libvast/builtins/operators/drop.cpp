//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder_factory.hpp>
#include <vast/type.hpp>

#include <arrow/type.h>

namespace vast::plugins::drop {

namespace {

/// The configuration of a project pipeline operator.
struct configuration {
  /// The key suffixes of the fields to drop.
  std::vector<std::string> fields = {};

  /// The key suffixes of the schemas to drop.
  std::vector<std::string> schemas = {};

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.fields, x.schemas);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"fields", list_type{string_type{}}},
      {"schemas", list_type{string_type{}}},
    };
    return result;
  }
};

/// Drops the specifed fields from the input.
class drop_operator : public pipeline_operator {
public:
  explicit drop_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_DEBUG("drop operator adds batch");
    // Determine whether we want to drop the entire batch first.
    const auto drop_schema
      = std::any_of(config_.schemas.begin(), config_.schemas.end(),
                    [&](const auto& dropped_schema) {
                      return dropped_schema == layout.name();
                    });
    if (drop_schema)
      return caf::none;
    // Apply the transformation.
    auto transform_fn
      = [&](struct record_type::field, std::shared_ptr<arrow::Array>) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      return {};
    };
    auto transformations = std::vector<indexed_transformation>{};
    for (const auto& field : config_.fields)
      for (auto&& index : caf::get<record_type>(layout).resolve_key_suffix(
             field, layout.name()))
        transformations.push_back({std::move(index), transform_fn});
    // transform_columns requires the transformations to be sorted, and that may
    // not necessarily be true if we have multiple fields configured, so we sort
    // again in that case.
    if (config_.fields.size() > 1)
      std::sort(transformations.begin(), transformations.end());
    auto [adjusted_layout, adjusted_batch]
      = transform_columns(layout, batch, transformations);
    if (adjusted_layout) {
      VAST_ASSERT(adjusted_batch);
      transformed_.emplace_back(std::move(adjusted_layout),
                                std::move(adjusted_batch));
    }
    return caf::none;
  }

  caf::expected<std::vector<pipeline_batch>> finish() override {
    VAST_DEBUG("drop operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_;

  /// The underlying configuration of the transformation.
  configuration config_;
};

class plugin final : public virtual pipeline_operator_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "drop";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (!(options.contains("fields") || options.contains("schemas")))
      return caf::make_error(ec::invalid_configuration,
                             "key 'fields' or 'schemas' is missing in "
                             "configuration for drop operator");
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<drop_operator>(std::move(*config));
  }

  virtual std::pair<std::string_view::iterator,
                    caf::expected<std::unique_ptr<pipeline_operator>>>
  parse_pipeline_string(std::string_view str) const override {
    record options;
    auto extractor_list = vast::list{};
    auto parsing_word = false;
    auto maybe_last_word = false;
    std::string current_extractor;
    auto str_r_it = str.begin();
    for (auto str_l_it = str_r_it; str_l_it != str.end() && *str_l_it != '|';
         ++str_r_it) {
      if (std::isspace(*str_r_it)) {
        if (parsing_word && !maybe_last_word) {
          maybe_last_word = true;
          current_extractor = {str_l_it, str_r_it};
        }
      } else if (*str_r_it == ','
                 || (str_r_it == str.end() || *str_r_it == '|')) {
        if (parsing_word) {
          parsing_word = false;
          if (maybe_last_word) {
            maybe_last_word = false;
          } else {
            current_extractor = {str_l_it, str_r_it};
          }
          extractor_list.emplace_back(current_extractor);
          str_l_it = str_r_it;
        } else {
          return {str_r_it, caf::make_error(ec::parse_error,
                                            "comma not delimiting extractors")};
        }
      } else {
        if (maybe_last_word) {
          return {str_r_it, caf::make_error(ec::parse_error, "extractors must "
                                                             "be separated by "
                                                             "a comma")};
        }
        if (!parsing_word) {
          str_l_it = str_r_it;
          parsing_word = true;
        }
      }
    }
    options["fields"] = extractor_list;
    options["schemas"] = extractor_list;
    return std::pair{str_r_it, make_pipeline_operator(options)};
  }
};

} // namespace

} // namespace vast::plugins::drop

VAST_REGISTER_PLUGIN(vast::plugins::drop::plugin)
