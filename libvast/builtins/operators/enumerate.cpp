//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/vast/option_set.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_builder.hpp>

#include <arrow/type.h>

namespace vast::plugins::enumerate {

namespace {

// TODO: refactor this once we have a modifier that allows for toggling
// between intra-schema and inter-schema processing.
class enumerate_operator final : public crtp_operator<enumerate_operator> {
public:
  explicit enumerate_operator(std::string field) : field_{std::move(field)} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    // Per-schema state.
    auto current_type = type{};
    std::unordered_map<type, uint64_t> offsets;
    // Create transformation to prepend  column to slice.
    auto transformations = std::vector<indexed_transformation>{};
    auto function = [&](struct record_type::field field,
                        std::shared_ptr<arrow::Array> array) mutable
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      // Create a column with the RIDs.
      auto n = array->length();
      auto rid_type = uint64_type{};
      auto builder
        = uint64_type::make_arrow_builder(arrow::default_memory_pool());
      auto reserve_result = builder->Reserve(n);
      VAST_ASSERT_CHEAP(reserve_result.ok(), reserve_result.ToString().c_str());
      // Fill the column.
      auto& offset = offsets[current_type];
      for (uint64_t i = 0; i < detail::narrow_cast<uint64_t>(n); ++i) {
        auto append_result
          = append_builder(rid_type, *builder, view<uint64_t>{offset + i});
        VAST_ASSERT(append_result.ok(), append_result.ToString().c_str());
      }
      offset += n;
      // Replace first column with a pair of (RID, first).
      auto rid_array = builder->Finish().ValueOrDie();
      return {
        {{field_, rid_type}, rid_array},
        {field, array},
      };
    };
    transformations.emplace_back(offset{0}, std::move(function));
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      };
      current_type = slice.schema();
      co_yield transform_columns(slice, transformations);
    }
  }

  auto to_string() const -> std::string override {
    return "enumerate";
  }

private:
  std::string field_;
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "enumerate";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();

    const auto options = option_set_parser{{{"field", 'f'}}};
    const auto option_parser = (required_ws_or_comment >> options);
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (!option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse enumerate operator "
                                    "options: '{}'",
                                    pipeline)),
      };
    }
    const auto p = optional_ws_or_comment >> end_of_pipeline_operator;
    if (!p(f, l, unused)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "enumerate operator: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    std::string field;
    for (auto& [key, value] : parsed_options)
      if (key == "f" || key == "field")
        if (auto* f = caf::get_if<std::string>(&value))
          field = std::move(*f);
    return {
      std::string_view{f, l},
      std::make_unique<enumerate_operator>(std::move(field)),
    };
  }
};

} // namespace

} // namespace vast::plugins::enumerate

VAST_REGISTER_PLUGIN(vast::plugins::enumerate::plugin)
