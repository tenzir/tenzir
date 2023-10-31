//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aliases.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/uuid.hpp>
#include <tenzir/view.hpp>

#include <arrow/array/builder_base.h>
#include <arrow/array/builder_nested.h>
#include <arrow/buffer_builder.h>
#include <arrow/record_batch.h>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <string>
#include <unordered_set>

namespace tenzir::plugins::lookup_tables {
class hash_table_plugin : public virtual lookup_table_plugin {
public:
  auto name() const -> std::string override {
    return "hash-table";
  }
  auto
  apply_lookup(std::vector<table_slice> slices,
               std::unordered_set<std::string> fields, record indicators) const
    -> std::vector<table_slice> override {
    auto hash_map = tsl::robin_map<data, std::string>{};
    for (const auto& [context, data] : indicators) {
      hash_map[data] = context;
    }
    if (indicators.empty()) {
      return {};
    }
    std::vector<table_slice> new_slices;
    for (auto&& slice : slices) {
      if (not caf::holds_alternative<record_type>(slice.schema())) {
        continue;
      }
      auto resolved_slice = resolve_enumerations(slice);
      auto t = caf::get<record_type>(resolved_slice.schema());
      auto found_indicators = std::set<std::string>{};
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      for (const auto& row : values(t, *array)) {
        for (const auto& a : *row) {
          if (not fields.empty()
              and fields.find(std::string{a.first}) == fields.end()) {
            continue;
          }
          if (auto it = hash_map.find(a.second); it != hash_map.end()) {
            found_indicators.emplace(it->second);
          }
        }
        if (not found_indicators.empty()) {
          auto l = list{found_indicators.begin(), found_indicators.end()};
          const auto result_schema = type{
            "tenzir.sighting",
            record_type{
              {"event", resolved_slice.schema()},
              {"indicators", type{list_type{string_type{}}}},
            },
          };
          auto result_builder
            = result_schema.make_arrow_builder(arrow::default_memory_pool());
          for (const auto& row :
               values(caf::get<record_type>(resolved_slice.schema()), *array)) {
            const auto append_row_result
              = caf::get<arrow::StructBuilder>(*result_builder).Append();
            TENZIR_ASSERT(append_row_result.ok());
            const auto append_event_result = append_builder(
              caf::get<record_type>(resolved_slice.schema()),
              caf::get<arrow::StructBuilder>(
                *caf::get<arrow::StructBuilder>(*result_builder)
                   .field_builder(0)),
              *row);
            TENZIR_ASSERT(append_event_result.ok());
            const auto append_indicators_result = append_builder(
              list_type{string_type{}},
              caf::get<arrow::ListBuilder>(
                *caf::get<arrow::StructBuilder>(*result_builder)
                   .child_builder(1)),
              caf::get<view<list>>(make_data_view(l)));
            TENZIR_ASSERT(append_indicators_result.ok());
          }
          auto result = result_builder->Finish().ValueOrDie();
          auto rb = arrow::RecordBatch::Make(
            result_schema.to_arrow_schema(), resolved_slice.rows(),
            caf::get<arrow::StructArray>(*result).fields());
          new_slices.emplace_back(rb, result_schema);
        }
      }
    }
    return new_slices;
  }
};

} // namespace tenzir::plugins::lookup_tables

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lookup_tables::hash_table_plugin)
