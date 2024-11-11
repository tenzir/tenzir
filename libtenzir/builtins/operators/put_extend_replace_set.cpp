//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/error.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <fmt/format.h>

namespace tenzir::plugins::put_extend_replace {

namespace {

enum class mode {
  put,
  extend,
  replace,
  set,
};

constexpr auto operator_name(enum mode mode) -> std::string_view {
  switch (mode) {
    case mode::put:
      return "put";
    case mode::extend:
      return "extend";
    case mode::replace:
      return "replace";
    case mode::set:
      return "set";
  }
  return "<unknown>";
}

/// The parsed configuration.
struct configuration {
  std::vector<std::tuple<std::string, caf::optional<operand>>>
    extractor_to_operand = {};

  friend auto inspect(auto& f, configuration& x) -> bool {
    return f.apply(x.extractor_to_operand);
  }
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
        diagnostic::warning("duplicate or conflicting assignment for field {}",
                            field)
          .hint("schema {}", slice.schema())
          .note("from `{}`", operator_name(Mode))
          .emit(ctrl.diagnostics());
        continue;
      }
      if (not operand) {
        auto field_as_operand = to<tenzir::operand>(field);
        if (not field_as_operand) {
          field_as_operand = data{};
        }
        operand.emplace(std::move(*field_as_operand));
      }
      auto [type, array] = resolve_operand(slice, *operand);
      if (not type && not array) {
        diagnostic::error(
          "sorting across heterogeneous lists is not implemented")
          .note("from `{}`", operator_name(Mode))
          .emit(ctrl.diagnostics());
        continue;
      }
      result.insert(result.begin(), {{field, type}, array});
    }
    if (not override) {
      result.insert(result.begin(),
                    {std::move(input_field), std::move(input_array)});
    }
    return result;
  };
}

auto make_replace(const table_slice& slice, const operand& op,
                  operator_control_plane& ctrl) {
  return [&](struct record_type::field input_field,
             std::shared_ptr<arrow::Array>)
           -> std::vector<std::pair<struct record_type::field,
                                    std::shared_ptr<arrow::Array>>> {
    auto [type, array] = resolve_operand(slice, op);
    if (not type && not array) {
      diagnostic::error("lists must have a homogeneous element type")
        .note("from `{}`", operator_name(mode::replace))
        .emit(ctrl.diagnostics());
      return {};
    }
    return {{
      {input_field.name, type},
      array,
    }};
  };
}

template <mode Mode>
class put_extend_operator final
  : public crtp_operator<put_extend_operator<Mode>> {
public:
  put_extend_operator() = default;

  explicit put_extend_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto name() const -> std::string override {
    return std::string{operator_name(Mode)};
  }

  auto operator()(const table_slice& slice, operator_control_plane& ctrl) const
    -> table_slice {
    if (slice.rows() == 0)
      return {};
    const auto& layout = as<record_type>(slice.schema());
    auto transformations1 = std::vector<indexed_transformation>{};
    auto transformations2 = std::vector<indexed_transformation>{};
    auto replace_schema_name = std::optional<std::string>{};
    // The additional assignments config needs to live until after we call
    // transform_columns, and is only relevant for set and put. It has to be in
    // this outer scope.
    auto modified_config = configuration{};
    switch (Mode) {
      case mode::put: {
        // For `put` we drop all fields except for the last one, and then
        // replace the last with the new one.
        modified_config = config_;
        std::erase_if(modified_config.extractor_to_operand, [&](const auto& x) {
          const auto& [extractor, operand] = x;
          if (extractor == "#schema") {
            replace_schema_name = as<std::string>(as<data>(*operand));
            return true;
          }
          return false;
        });
        // If we only rename the schema then we have no fields left, which we
        // special-case here. That's not good, but better than crashing.
        if (modified_config.extractor_to_operand.empty()) {
          for (size_t i = 0; i < layout.num_fields(); ++i) {
            transformations1.push_back({{i}, make_drop()});
          }
          break;
        }
        for (size_t i = 0; i < layout.num_fields() - 1; ++i) {
          transformations1.push_back({{i}, make_drop()});
        }
        auto duplicates = std::unordered_set<std::string>{};
        duplicates.reserve(modified_config.extractor_to_operand.size());
        const auto override = true;
        transformations1.push_back(
          {{layout.num_fields() - 1},
           make_extend<Mode>(slice, modified_config, ctrl,
                             std::move(duplicates), override)});
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
        transformations1.push_back(
          {{layout.num_fields() - 1},
           make_extend<Mode>(slice, config_, ctrl, std::move(duplicates),
                             override)});
        break;
      }
      case mode::replace:
      case mode::set: {
        // For `replace` we need to treat the field as an extractor. For `set`,
        // we additionally extend with the remaining extractors.
        auto index_to_operand
          = std::vector<std::pair<offset, const operand*>>{};
        for (const auto& [extractor, operand] : config_.extractor_to_operand) {
          if (extractor == "#schema") {
            TENZIR_ASSERT(operand);
            replace_schema_name = as<std::string>(as<data>(*operand));
            continue;
          }
          if (not operand) {
            diagnostic::warning("ignoring implicit assignment for field `{}`",
                                extractor)
              .note("from `{}`", operator_name(Mode))
              .emit(ctrl.diagnostics());
            continue;
          }
          auto resolved = false;
          for (const auto& index : slice.schema().resolve(extractor)) {
            index_to_operand.emplace_back(index, &*operand);
            resolved = true;
          }
          if (not resolved and Mode == mode::set
              and not extractor.starts_with(':')) {
            modified_config.extractor_to_operand.emplace_back(extractor,
                                                              operand);
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
          transformations1.push_back(
            {index, make_replace(slice, *operand, ctrl)});
        }
        if (not modified_config.extractor_to_operand.empty()) {
          transformations2.push_back(
            {{layout.num_fields() - 1},
             make_extend<Mode>(slice, modified_config, ctrl, {}, false)});
        }
        break;
      }
    }
    // Lastly, apply our transformations.
    auto result = transform_columns(transform_columns(slice, transformations1),
                                    transformations2);
    if (replace_schema_name) {
      result = cast(result, type{*replace_schema_name, result.schema()});
    } else if (Mode == mode::put) {
      auto renamed_schema
        = type{"tenzir.put", as<record_type>(result.schema())};
      result = cast(std::move(result), renamed_schema);
    }
    return result;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, put_extend_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

template <mode Mode>
class plugin final : public virtual operator_plugin<put_extend_operator<Mode>> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

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
      >> (((extractor | parsers::str{"#schema"}) >> -(optional_ws_or_comment >> '=' >> optional_ws_or_comment >> operand))
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
    for (auto& [ex, op] : config.extractor_to_operand) {
      if (ex == "#schema") {
        if constexpr (Mode != mode::extend) {
          auto* op_ptr = op ? &*op : nullptr;
          // FIXME: Chaining `caf::get_if` leads to a segfault.
          auto* data_ptr = op_ptr ? caf::get_if<data>(op_ptr) : nullptr;
          auto* str_ptr
            = data_ptr ? caf::get_if<std::string>(data_ptr) : nullptr;
          if (not str_ptr) {
            return {
              std::string_view{f, l},
              caf::make_error(ec::syntax_error,
                              fmt::format("assignment to `#schema` must be a "
                                          "string literal")),
            };
          }
        } else {
          return {
            std::string_view{f, l},
            caf::make_error(ec::syntax_error,
                            fmt::format("`{}` does not support `#schema`",
                                        operator_name(Mode))),
          };
        }
      }
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
using set_plugin = plugin<mode::set>;

} // namespace

} // namespace tenzir::plugins::put_extend_replace

TENZIR_REGISTER_PLUGIN(tenzir::plugins::put_extend_replace::put_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::put_extend_replace::extend_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::put_extend_replace::replace_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::put_extend_replace::set_plugin)
