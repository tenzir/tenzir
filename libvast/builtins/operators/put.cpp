//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/overload.hpp>
#include <vast/error.hpp>
#include <vast/legacy_pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice_builder.hpp>
#include <vast/type.hpp>

#include <arrow/array.h>
#include <fmt/format.h>

namespace vast::plugins::put {

namespace {

/// The parsed configuration.
struct configuration {
  std::vector<std::tuple<std::string, caf::optional<operand>>> field_to_operand
    = {};
};

class put_operator final : public crtp_operator<put_operator> {
public:
  explicit put_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto operator()(const table_slice& slice, operator_control_plane& ctrl) const
    -> table_slice {
    if (slice.rows() == 0)
      return {};
    const auto& layout = caf::get<record_type>(slice.schema());
    auto batch = to_record_batch(slice);
    VAST_ASSERT(batch);
    std::vector<indexed_transformation> transformations = {};
    // We drop all fields except for the last one...
    for (size_t i = 0; i < layout.num_fields() - 1; ++i) {
      static auto drop =
        [](struct record_type::field, std::shared_ptr<arrow::Array>)
        -> std::vector<
          std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
        return {};
      };
      transformations.push_back({{i}, drop});
    }
    // ... and then replace the last one with our new fields.
    auto put = [&](struct record_type::field, std::shared_ptr<arrow::Array>) {
      // For each field in the configuration, we want to create a new field.
      auto result = std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{};
      result.reserve(config_.field_to_operand.size());
      auto duplicates = std::unordered_set<std::string>{};
      for (auto it = config_.field_to_operand.rbegin();
           it < config_.field_to_operand.rend(); ++it) {
        auto [field, operand] = *it;
        if (not duplicates.insert(field).second) {
          ctrl.warn(caf::make_error(
            ec::invalid_argument, fmt::format("put operator ignores duplicate "
                                              "assignment for field {}",
                                              field)));
          continue;
        }
        if (not operand) {
          auto field_as_operand = to<vast::operand>(field);
          if (not field_as_operand) {
            ctrl.warn(caf::make_error(
              ec::logic_error,
              fmt::format("put operator failed to parse field as extractor in "
                          "implicit assignment for field {}, and assigns null",
                          field)));
            field_as_operand = data{};
          }
          operand.emplace(std::move(*field_as_operand));
        }
        auto [type, array] = resolve_operand(slice, *operand);
        result.insert(result.begin(), {{field, type}, array});
      }
      return result;
    };
    transformations.push_back({{layout.num_fields() - 1}, std::move(put)});
    return transform_columns(slice, transformations);
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    auto result = std::string{"put"};
    bool first = true;
    for (const auto& [field, operand] : config_.field_to_operand) {
      if (not std::exchange(first, false)) {
        result += ',';
      }
      fmt::format_to(std::back_inserter(result), " {}", field);
      if (operand) {
        fmt::format_to(std::back_inserter(result), "={}", *operand);
      }
    }
    return result;
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "put";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::identifier, parsers::operand;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // put <field=operand>...
    // clang-format off
    const auto p
       = required_ws_or_comment
      >> ((identifier >> -(optional_ws_or_comment >> '=' >> optional_ws_or_comment >> operand))
        % (optional_ws_or_comment >> ',' >> optional_ws_or_comment))
      >> optional_ws_or_comment
      >> end_of_pipeline_operator;
    // clang-format on
    auto config = configuration{};
    if (!p(f, l, config.field_to_operand)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse put operator: '{}'",
                                    pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<put_operator>(std::move(config)),
    };
  }
};

} // namespace

} // namespace vast::plugins::put

VAST_REGISTER_PLUGIN(vast::plugins::put::plugin)
