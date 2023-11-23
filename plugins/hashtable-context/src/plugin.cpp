//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/project.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/typed_array.hpp>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <string>

namespace tenzir::plugins::hashtable_context {

namespace {

class ctx : public virtual context {
public:
  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, record parameters) const
    -> caf::expected<typed_array> override {
    // fields are in parameters
    if (not caf::holds_alternative<record_type>(slice.schema())) {
      return typed_array{null_type{}, {}};
    }
    auto resolved_slice = resolve_enumerations(slice);
    auto t = caf::get<record_type>(resolved_slice.schema());
    auto found_indicators = std::vector<table_slice>{};
    auto array = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();

    auto fields = caf::get_if<list>(&parameters["fields"]);
    if (!fields) {
      // todo return null array of length slice.rows().
      // Or decide what to do here ...
    }
    for (auto field : *fields) {
      auto column_offset = caf::get<record_type>(slice.schema()).resolve_prefix(caf::get<std::string>(field));
      auto [type, array] = column.get(resolved_slice);
      for (auto value : values(type, *array)) {
        if (auto it = offset_table.find(value); it != offset_table.end()) {
      }
    }
    for (const auto& row : values(t, *array)) {
      for (const auto& a : *row) {
        if (not parameters.empty()
            and not parameters.contains(std::string{a.first})) {
          continue;
        }
        if (auto it = offset_table.find(a.second); it != offset_table.end()) {
          found_indicators.emplace_back(*slice_table.lookup(it->second));
        }
      }
      if (not found_indicators.empty()) {
        // schema: context & timestamp of hit
        const auto result_schema = type{
          "tenzir.context.hit",
          record_type{
            {"event", list_type{found_indicators.front().schema()}},
            {"timestamp", type{duration_type{}}},
          },
        };
        builder = series_builder{};
        auto guard = builder.record();
        guard.field("context", 
        //auto result_builder = resolved_slice.schema().make_arrow_builder(
        //  arrow::default_memory_pool());
        for (const auto& row :
             values(caf::get<record_type>(resolved_slice.schema()), *array)) {
          const auto append_row_result
            = caf::get<arrow::StructBuilder>(*result_builder).Append();
          TENZIR_ASSERT(append_row_result.ok());
          const auto append_event_result = append_builder(
            caf::get<record_type>(resolved_slice.schema()),
            caf::get<arrow::StructBuilder>(
              *caf::get<arrow::StructBuilder>(*result_builder).field_builder(0)),
            *row);
          TENZIR_ASSERT(append_event_result.ok());
          const auto append_indicators_result = append_builder(
            duration_type{},
            caf::get<arrow::NumericBuilder<arrow::DurationType>>(
              *caf::get<arrow::StructBuilder>(*result_builder).child_builder(1)),
            caf::get<view<duration>>(make_data_view(
              std::chrono::system_clock::now().time_since_epoch())));
          TENZIR_ASSERT(append_indicators_result.ok());
        }
        auto result = result_builder->Finish().ValueOrDie();
        return typed_array{result_schema, result};
      }
    }
    return typed_array{null_type{}, {}};
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{{"TODO", "unimplemented"}};
  }

  /// Updates the context.
  auto update(table_slice slice, record parameters)
    -> caf::expected<record> override {
    // context does stuff on its own with slice & parameters
    if (parameters.contains("clear")) {
      auto* clear = get_if<bool>(&parameters, "clear");
      if (clear and *clear) {
        current_offset = 0;
        offset_table.clear();
        slice_table.clear();
      }
    }
    if (slice.rows() == 0) {
      TENZIR_INFO("got an empty slice");
      return record{};
    }
    // slice schema for this update is:
    /*
        {
          "key": string,
          "context" : data,
          "retire": bool,
        }
        all of them arrays
        if not = diagnostic & ignore.
    */
    //auto t = caf::get<record_type>(slice.schema());
    //auto array = to_record_batch(slice)->ToStructArray().ValueOrDie();
    for (auto i = current_offset; i < (current_offset + slice.rows()); ++i) {
      auto v = slice.at(0, i);
      offset_table[materialize(v)] = i;
    }
    slice_table.insert(current_offset, current_offset + slice.rows(), slice);
    current_offset += slice.rows();

    return record{{"updated", slice.rows()}};
  }

  auto update(chunk_ptr bytes, record parameters)
    -> caf::expected<record> override {
    return ec::unimplemented;
  }

  auto update(record parameters)
    -> caf::expected<record> override {
    return ec::unimplemented;
  }

  auto save() const
    -> caf::expected<chunk_ptr> override {
    return ec::unimplemented;
  }

private:
  tsl::robin_map<data, size_t> offset_table;
  detail::range_map<size_t, table_slice> slice_table;
  size_t current_offset = 0;
};

class plugin : public virtual context_plugin {
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)plugin_config;
    (void)global_config;
    return caf::none;
  }

  auto name() const -> std::string override {
    return "hashtable-context";
  }

  auto make_context(record parameters) const
    -> caf::expected<std::unique_ptr<context>> override {
    (void)parameters;
    return std::make_unique<ctx>();
  }

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    // do something...
    return std::make_unique<ctx>();
  }
};

} // namespace

} // namespace tenzir::plugins::hashtable_context

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hashtable_context::plugin)
