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
#include <vast/concept/parseable/core.hpp>
#include <vast/concept/parseable/string/char_class.hpp>
#include <vast/detail/inspection_common.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/hash/default_hash.hpp>
#include <vast/hash/hash_append.hpp>
#include <vast/optional.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>

#include <arrow/scalar.h>
#include <fmt/format.h>

namespace vast::plugins::hash {

namespace {

/// The configuration of the hash pipeline operator.
struct configuration {
  std::string field;
  std::string out;
  std::optional<std::string> salt;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.field, x.out, x.salt);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"field", string_type{}},
      {"out", string_type{}},
      {"salt", string_type{}},
    };
    return result;
  }
};

class hash_operator : public pipeline_operator {
public:
  explicit hash_operator(configuration configuration);

  [[nodiscard]] caf::error
  add(type schema, std::shared_ptr<arrow::RecordBatch> batch) override {
    VAST_TRACE("hash operator adds batch");
    // Get the target field if it exists.
    const auto& schema_rt = caf::get<record_type>(schema);
    auto column_index = schema_rt.resolve_key(config_.field);
    if (!column_index) {
      transformed_.emplace_back(schema, std::move(batch));
      return caf::none;
    }
    // Apply the transformation.
    auto transform_fn = [&](struct record_type::field field,
                            std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto hashes_builder
        = string_type::make_arrow_builder(arrow::default_memory_pool());
      if (config_.salt) {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = vast::hash(value, *config_.salt);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
      } else {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = vast::hash(value);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
      }
      return {
        {
          std::move(field),
          std::move(array),
        },
        {
          {
            config_.out,
            string_type{},
          },
          hashes_builder->Finish().ValueOrDie(),
        },
      };
    };
    auto [adjusted_schema, adjusted_batch] = transform_columns(
      schema, batch, {{*column_index, std::move(transform_fn)}});
    VAST_ASSERT(adjusted_schema);
    VAST_ASSERT(adjusted_batch);
    transformed_.emplace_back(std::move(adjusted_schema),
                              std::move(adjusted_batch));
    return caf::none;
  }

  [[nodiscard]] caf::expected<std::vector<pipeline_batch>> finish() override {
    VAST_DEBUG("hash operator finished transformation");
    return std::exchange(transformed_, {});
  }

private:
  /// The slices being transformed.
  std::vector<pipeline_batch> transformed_;

  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

hash_operator::hash_operator(configuration configuration)
  : config_(std::move(configuration)) {
}

class plugin final : public virtual pipeline_operator_plugin {
public:
  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "hash";
  };

  // transform plugin API
  [[nodiscard]] caf::expected<std::unique_ptr<pipeline_operator>>
  make_pipeline_operator(const record& options) const override {
    if (!options.contains("field"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'field' is missing in configuration for hash "
                             "operator");
    if (!options.contains("out"))
      return caf::make_error(ec::invalid_configuration,
                             "key 'out' is missing "
                             "in configuration for hash operator");
    auto config = to<configuration>(options);
    if (!config)
      return config.error();
    return std::make_unique<hash_operator>(std::move(*config));
  }

  [[nodiscard]] std::pair<std::string_view,
                          caf::expected<std::unique_ptr<pipeline_operator>>>
  make_pipeline_operator(std::string_view pipeline) const override {
    auto short_form_options = std::map<std::string, std::string>{};
    auto long_form_options = std::map<std::string, std::string>{};
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    using parsers::space, parsers::eoi, parsers::alnum, parsers::alpha,
      parsers::chr;
    using namespace parser_literals;
    const auto required_ws = ignore(+space);
    const auto optional_ws = ignore(*space);

    auto extractor_char = alnum | chr{'_'} | chr{'-'} | chr{':'};
    auto extractor
      = (!('-'_p) >> (+extractor_char % '.'))
          .then([](std::vector<std::string> in) {
            return fmt::to_string(fmt::join(in.begin(), in.end(), "."));
          });
    auto short_form_option_key = (alpha);
    auto long_form_option_key = ('-'_p) >> (+alpha);
    auto option_value = (+extractor_char);
    auto short_form_option
      = (short_form_option_key >> (+space) >> option_value)
          .then([&short_form_options](std::string in) {
            auto split_options = detail::split(in, " ");
            short_form_options[std::string{split_options.front()}]
              = split_options.back();
            return in;
          });
    auto long_form_option
      = (long_form_option_key >> optional_ws >> chr{'='} >> optional_ws
         >> option_value)
          .then([&long_form_options](std::string in) {
            auto split_options = detail::split(in, "=");
            long_form_options[std::string{split_options.front()}]
              = split_options.back();
            return in;
          });
    auto option = ('-') >> (short_form_option | long_form_option);
    const auto option_parser = (required_ws >> (option % (required_ws)));
    if (!option_parser(f, l, unused)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse hash "
                                                      "operator options: '{}'",
                                                      pipeline)),
      };
    }
    const auto extractor_parser = optional_ws
                                  >> (extractor % (',' >> optional_ws))
                                  >> optional_ws >> ('|' | eoi);
    auto parsed_extractors = std::vector<std::string>{};
    if (!extractor_parser(f, l, parsed_extractors)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse hash "
                                                      "operator extractor: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    config.field = parsed_extractors.front();
    config.out = parsed_extractors.front() + "_hashed";
    if (!short_form_options["s"].empty()) {
      config.salt = short_form_options["s"];
    } else if (!long_form_options["salt"].empty()) {
      config.salt = long_form_options["salt"];
    }
    return {
      std::string_view{f, l},
      std::make_unique<hash_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::hash

VAST_REGISTER_PLUGIN(vast::plugins::hash::plugin)
