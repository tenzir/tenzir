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
#include <vast/detail/escapers.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/detail/string.hpp>
#include <vast/error.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>
#include <vast/table_slice_builder.hpp>

#include <arrow/type.h>

#include <unordered_set>

namespace vast::plugins::enumerate {

namespace {

// TODO: refactor this once we have a modifier that allows for toggling
// between intra-schema and inter-schema processing.
class enumerate_operator final : public crtp_operator<enumerate_operator> {
  static constexpr auto default_field_name = "#";

public:
  explicit enumerate_operator(std::string field) : field_{std::move(field)} {
    if (field_.empty())
      field_ = default_field_name;
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto current_type = type{};
    std::unordered_map<type, uint64_t> offsets;
    std::unordered_set<type> skipped_schemas;
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
    transformations.push_back({offset{0}, std::move(function)});
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
      } else if (skipped_schemas.contains(slice.schema())) {
        co_yield slice;
      } else if (caf::get<record_type>(slice.schema())
                   .resolve_key(field_)
                   .has_value()) {
        ctrl.warn(caf::make_error(ec::unspecified,
                                  fmt::format("ignoring schema {} with already "
                                              "existing enumeration key {}",
                                              slice.schema().name(), field_)));
        skipped_schemas.insert(slice.schema());
        co_yield slice;
      } else {
        current_type = slice.schema();
        co_yield transform_columns(slice, transformations);
      }
    }
  }

  auto to_string() const -> std::string override {
    if (field_ == default_field_name)
      return "enumerate";
    // We may want to factor this so that it can be used by other operators.
    auto escaper = [](auto& f, auto out) {
      auto escape_char = [](char c, auto out) {
        *out++ = '\\';
        *out++ = c;
      };
      switch (*f) {
        default:
          *out++ = *f++;
          return;
        case '\\':
          escape_char('\\', out);
          break;
        case '"':
          escape_char('"', out);
          break;
      }
      ++f;
    };
    return fmt::format("enumerate \"{}\"", detail::escape(field_, escaper));
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
      parsers::optional_ws_or_comment, parsers::operator_arg;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> operator_arg)
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::string field;
    if (!p(f, l, field)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "enumerate operator: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<enumerate_operator>(std::move(field)),
    };
  }
};

} // namespace

} // namespace vast::plugins::enumerate

VAST_REGISTER_PLUGIN(vast::plugins::enumerate::plugin)
