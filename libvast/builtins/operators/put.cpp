//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/overload.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/narrow.hpp>
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

auto bind_operand(std::string field, table_slice slice, operand op)
  -> std::pair<struct record_type::field, std::shared_ptr<arrow::Array>> {
  VAST_ASSERT(slice.rows() > 0);
  const auto batch = to_record_batch(slice);
  const auto& layout = caf::get<record_type>(slice.schema());
  auto inferred_type = type{};
  auto array = std::shared_ptr<arrow::Array>{};
  auto bind_value = [&](const data& value) {
    inferred_type = type::infer(value);
    if (not inferred_type) {
      auto builder
        = string_type::make_arrow_builder(arrow::default_memory_pool());
      const auto append_result = builder->AppendNulls(batch->num_rows());
      VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
      array = builder->Finish().ValueOrDie();
      return;
    }
    auto g = [&]<concrete_type Type>(const Type& inferred_type) {
      auto builder
        = inferred_type.make_arrow_builder(arrow::default_memory_pool());
      for (int i = 0; i < batch->num_rows(); ++i) {
        const auto append_result
          = append_builder(inferred_type, *builder,
                           make_view(caf::get<type_to_data_t<Type>>(value)));
        VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
      }
      array = builder->Finish().ValueOrDie();
    };
    caf::visit(g, inferred_type);
  };
  auto f = detail::overload{
    [&](const data& value) {
      bind_value(value);
    },
    [&](const field_extractor& ex) {
      for (const auto& index :
           layout.resolve_key_suffix(ex.field, slice.schema().name())) {
        inferred_type = layout.field(index).type;
        array = static_cast<arrow::FieldPath>(index).Get(*batch).ValueOrDie();
        return;
      }
      bind_value({});
    },
    [&](const type_extractor& ex) {
      for (const auto& [field, index] : layout.leaves()) {
        bool match = field.type == ex.type;
        if (not match) {
          for (auto name : field.type.names()) {
            if (name == ex.type.name()) {
              match = true;
              break;
            }
          }
        }
        if (match) {
          inferred_type = field.type;
          array = static_cast<arrow::FieldPath>(index).Get(*batch).ValueOrDie();
          return;
        }
      }
      bind_value({});
    },
    [&](const meta_extractor& ex) {
      switch (ex.kind) {
        case meta_extractor::type: {
          bind_value(std::string{slice.schema().name()});
          return;
        }
        case meta_extractor::import_time: {
          bind_value(slice.import_time());
          return;
        }
      }
      die("unhandled meta extractor kind");
    },
    [&](const data_extractor&) {
      die("data extractor must not occur here");
    },
  };
  caf::visit(f, op);
  return {
    {std::move(field), std::move(inferred_type)},
    std::move(array),
  };
}

class put_operator final : public crtp_operator<put_operator> {
public:
  explicit put_operator(configuration config) noexcept
    : config_{std::move(config)} {
    // nop
  }

  auto operator()(const table_slice& slice) const -> table_slice {
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
      for (const auto& [field, operand] : config_.field_to_operand) {
        result.push_back(
          bind_operand(field, slice, operand.value_or(field_extractor{field})));
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
