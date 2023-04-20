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

namespace vast::plugins::put_extend {

namespace {

enum class mode {
  put,
  extend,
};

constexpr auto operator_name(enum mode mode) -> std::string_view {
  switch (mode) {
    case mode::put:
      return "put";
    case mode::extend:
      return "extend";
  }
  return "<unknown>";
}

/// The parsed configuration.
struct configuration {
  std::vector<std::tuple<std::string, caf::optional<operand>>> field_to_operand
    = {};
};

template <mode Mode>
class put_extend_operator final
  : public crtp_operator<put_extend_operator<Mode>> {
public:
  explicit put_extend_operator(configuration config) noexcept
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
    auto duplicates = std::unordered_set<std::string>{};
    switch (Mode) {
      case mode::put: {
        // For `put` we drop all fields except for the last one...
        for (size_t i = 0; i < layout.num_fields() - 1; ++i) {
          static auto drop =
            [](struct record_type::field, std::shared_ptr<arrow::Array>)
            -> std::vector<std::pair<struct record_type::field,
                                     std::shared_ptr<arrow::Array>>> {
            return {};
          };
          transformations.push_back({{i}, drop});
        }
      }
      case mode::extend: {
        // For `extend` we instead consider all keys in the schema as
        // conflicting fields.
        for (const auto& leaf : layout.leaves()) {
          duplicates.insert(layout.key(leaf.index));
        }
      }
    }
    // ... and then replace the last one with our new fields.
    auto put = [&](struct record_type::field, std::shared_ptr<arrow::Array>) {
      // For each field in the configuration, we want to create a new field.
      auto result = std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{};
      result.reserve(config_.field_to_operand.size());
      for (auto it = config_.field_to_operand.rbegin();
           it < config_.field_to_operand.rend(); ++it) {
        auto [field, operand] = *it;
        if (not duplicates.insert(field).second) {
          ctrl.warn(caf::make_error(
            ec::invalid_argument,
            fmt::format("{} operator ignores duplicate or conflicting "
                        "assignment for field {} in schema {}",
                        operator_name(Mode), field, slice.schema())));
          continue;
        }
        if (not operand) {
          auto field_as_operand = to<vast::operand>(field);
          if (not field_as_operand) {
            ctrl.warn(caf::make_error(
              ec::logic_error,
              fmt::format("{} operator failed to parse field as extractor in "
                          "implicit assignment for field {}, and assigns null",
                          operator_name(Mode), field)));
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
    auto result = std::string{operator_name(Mode)};
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

template <mode Mode>
class plugin final : public virtual operator_plugin {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return std::string{operator_name(Mode)};
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::identifier, parsers::operand;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // put|extend <field=operand>...
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
                        fmt::format("failed to parse {} operator: '{}'",
                                    operator_name(Mode), pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<put_extend_operator<Mode>>(std::move(config)),
    };
  }
};

using put_plugin = plugin<mode::put>;
using extend_plugin = plugin<mode::extend>;

} // namespace

} // namespace vast::plugins::put_extend

VAST_REGISTER_PLUGIN(vast::plugins::put_extend::put_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::put_extend::extend_plugin)
