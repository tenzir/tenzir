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
  replace,
};

constexpr auto operator_name(enum mode mode) -> std::string_view {
  switch (mode) {
    case mode::put:
      return "put";
    case mode::extend:
      return "extend";
    case mode::replace:
      return "replace";
  }
  return "<unknown>";
}

/// The parsed configuration.
struct configuration {
  std::vector<std::tuple<std::string, caf::optional<operand>>>
    extractor_to_operand = {};
};

auto make_drop() {
  return
    [](struct record_type::field, std::shared_ptr<arrow::Array>)
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
    return {};
  };
}

template <mode Mode>
auto make_extend(const table_slice& slice, const configuration& config,
                 operator_control_plane& ctrl,
                 std::unordered_set<std::string> duplicates, bool override) {
  return [&, duplicates = std::move(duplicates),
          override](struct record_type::field input_field,
                    std::shared_ptr<arrow::Array> input_array) mutable {
    // For each field in the configuration, we want to create a new field.
    auto result = std::vector<
      std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>{};
    result.reserve(override ? config.extractor_to_operand.size()
                            : 1 + config.extractor_to_operand.size());
    for (auto it = config.extractor_to_operand.rbegin();
         it < config.extractor_to_operand.rend(); ++it) {
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
          field_as_operand = data{};
        }
        operand.emplace(std::move(*field_as_operand));
      }
      auto [type, array] = resolve_operand(slice, *operand);
      result.insert(result.begin(), {{field, type}, array});
    }
    if (not override) {
      result.insert(result.begin(),
                    {std::move(input_field), std::move(input_array)});
    }
    return result;
  };
}

auto make_replace(const table_slice& slice, const operand& op) {
  return [&](struct record_type::field input_field,
             std::shared_ptr<arrow::Array>)
           -> std::vector<std::pair<struct record_type::field,
                                    std::shared_ptr<arrow::Array>>> {
    auto resolved = resolve_operand(slice, op);
    return {{
      {input_field.name, resolved.first},
      resolved.second,
    }};
  };
}

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
    auto transformations = std::vector<indexed_transformation>{};
    switch (Mode) {
      case mode::put: {
        // For `put` we drop all fields except for the last one, and then
        // replace the last with the new one.
        for (size_t i = 0; i < layout.num_fields() - 1; ++i) {
          transformations.push_back({{i}, make_drop()});
        }
        auto duplicates = std::unordered_set<std::string>{};
        duplicates.reserve(config_.extractor_to_operand.size());
        const auto override = true;
        transformations.push_back(
          {{layout.num_fields() - 1},
           make_extend<Mode>(slice, config_, ctrl, std::move(duplicates),
                             override)});
        break;
      }
      case mode::extend: {
        // For `extend` we instead consider all keys in the schema as
        // conflicting fields.
        auto duplicates = std::unordered_set<std::string>{};
        duplicates.reserve(layout.num_leaves()
                           + config_.extractor_to_operand.size());
        for (const auto& leaf : layout.leaves()) {
          duplicates.insert(layout.key(leaf.index));
        }
        const auto override = false;
        transformations.push_back(
          {{layout.num_fields() - 1},
           make_extend<Mode>(slice, config_, ctrl, std::move(duplicates),
                             override)});
        break;
      }
      case mode::replace: {
        // For `replace` we need to treat the field as an extractor.
        auto index_to_operand
          = std::vector<std::pair<offset, const operand*>>{};
        for (const auto& [extractor, operand] : config_.extractor_to_operand) {
          if (not operand) {
            ctrl.warn(caf::make_error(
              ec::logic_error,
              fmt::format("{} operator ignores implicit implicit assignment "
                          "for extractor {}",
                          operator_name(Mode), extractor)));
            continue;
          }
          for (const auto& index :
               layout.resolve_key_suffix(extractor, slice.schema().name())) {
            index_to_operand.emplace_back(index, &*operand);
          }
        }
        // Remove adjacent duplicates.
        std::stable_sort(index_to_operand.begin(), index_to_operand.end(),
                         [](const auto& lhs, const auto& rhs) {
                           return lhs.first < rhs.first;
                         });
        const auto duplicate_it
          = std::unique(index_to_operand.begin(), index_to_operand.end(),
                        [](const auto& lhs, const auto& rhs) {
                          return lhs.first == rhs.first;
                        });
        index_to_operand.erase(duplicate_it, index_to_operand.end());
        // Create the transformation.
        for (const auto& [index, operand] : index_to_operand) {
          transformations.push_back({index, make_replace(slice, *operand)});
        }
        break;
      }
    }
    // Lastly, apply our transformation.
    return transform_columns(slice, transformations);
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    auto result = std::string{operator_name(Mode)};
    bool first = true;
    for (const auto& [field, operand] : config_.extractor_to_operand) {
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
      parsers::optional_ws_or_comment, parsers::extractor, parsers::operand;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    // put|extend <field=operand>...
    // replace <extractor=operand>...
    // clang-format off
    const auto p
       = required_ws_or_comment
      >> ((extractor >> -(optional_ws_or_comment >> '=' >> optional_ws_or_comment >> operand))
        % (optional_ws_or_comment >> ',' >> optional_ws_or_comment))
      >> optional_ws_or_comment
      >> end_of_pipeline_operator;
    // clang-format on
    auto config = configuration{};
    if (!p(f, l, config.extractor_to_operand)) {
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
using replace_plugin = plugin<mode::replace>;

} // namespace

} // namespace vast::plugins::put_extend

VAST_REGISTER_PLUGIN(vast::plugins::put_extend::put_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::put_extend::extend_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::put_extend::replace_plugin)
