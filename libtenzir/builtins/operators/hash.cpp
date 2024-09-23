//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/default_hash.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/optional.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <arrow/scalar.h>
#include <fmt/format.h>

namespace tenzir::plugins::hash {

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

class hash_operator final
  : public schematic_operator<hash_operator,
                              std::vector<indexed_transformation>> {
public:
  hash_operator() = default;

  explicit hash_operator(configuration configuration)
    : config_(std::move(configuration)) {
  }

  auto initialize(const type& schema, exec_ctx) const
    -> caf::expected<state_type> override {
    // Get the target field if it exists.
    auto column_index = schema.resolve_key_or_concept_once(config_.field);
    if (!column_index) {
      return state_type{};
    }
    auto transform_fn = [this](struct record_type::field field,
                               std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto hashes_builder
        = string_type::make_arrow_builder(arrow::default_memory_pool());
      if (config_.salt) {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = tenzir::hash(value, *config_.salt);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
      } else {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = tenzir::hash(value);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
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
    return state_type{{*column_index, std::move(transform_fn)}};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    return transform_columns(slice, state);
  };

  auto name() const noexcept -> std::string override {
    return "hash";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, hash_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual operator_plugin<hash_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto options = option_set_parser{{{"salt", 's'}}};
    const auto option_parser = (required_ws_or_comment >> options);
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (!option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse hash "
                                                      "operator options: '{}'",
                                                      pipeline)),
      };
    }
    const auto extractor_parser = optional_ws_or_comment >> extractor_list
                                  >> optional_ws_or_comment
                                  >> end_of_pipeline_operator;
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
    for (const auto& [key, value] : parsed_options) {
      auto value_str = caf::get_if<std::string>(&value);
      if (!value_str) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid option value "
                                                        "string for "
                                                        "pseudonymize "
                                                        "operator: "
                                                        "'{}'",
                                                        value)),
        };
      }
      if (key == "s" || key == "salt") {
        config.salt = *value_str;
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<hash_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::hash

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::plugin)
