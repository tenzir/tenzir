//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/numeric/bool.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/range_map.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/error.hpp>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::lookup_table {

namespace {

class ctx final : public virtual context {
public:
  ctx() noexcept = default;

  explicit ctx(tsl::robin_map<data, data> context_entries) noexcept
    : context_entries{std::move(context_entries)} {
    // nop
  }

  auto context_type() const -> std::string override {
    return "lookup-table";
  }

  auto apply(series s) const -> caf::expected<std::vector<series>> override {
    auto builder = series_builder{};
    for (const auto& value : s.values()) {
      if (auto it = context_entries.find(value); it != context_entries.end()) {
        builder.data(it->second);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }

  auto snapshot(parameter_map parameters) const
    -> caf::expected<expression> override {
    auto column = parameters["field"];
    auto keys = list{};
    keys.reserve(context_entries.size());
    auto first_index = std::optional<size_t>{};
    for (const auto& [k, _] : context_entries) {
      keys.emplace_back(k);
      auto current_index = k.get_data().index();
      if (not first_index) [[unlikely]] {
        first_index = current_index;
      } else if (*first_index != current_index) [[unlikely]] {
        // TODO: With the language revamp, we should get heterogeneous lookups
        // for free.
        return caf::make_error(ec::unimplemented,
                               "lookup-table does not support snapshots for "
                               "heterogeneous keys");
      }
    }
    return expression{
      predicate{
        field_extractor(*column),
        relational_operator::in,
        data{keys},
      },
    };
  }

  /// Inspects the context.
  auto show() const -> record override {
    return record{{"num_entries", context_entries.size()}};
  }

  auto dump() -> generator<table_slice> override {
    auto entry_builder = series_builder{};
    auto i = 0;
    for (const auto& [key, value] : context_entries) {
      entry_builder.data(record{{"key", key}, {"value", value}});
      ++i;
      if (i % 65536 == 0) {
        auto slices = entry_builder.finish_as_table_slice();
        for (auto&& slice : slices) {
          co_yield slice;
        }
      }
    }
    auto slices = entry_builder.finish_as_table_slice();
    for (auto&& slice : slices) {
      co_yield slice;
    }
  }

  /// Updates the context.
  auto update(table_slice slice, context::parameter_map parameters)
    -> caf::expected<update_result> override {
    // context does stuff on its own with slice & parameters
    if (parameters.contains("clear")) {
      auto clear = parameters["clear"];
      if (not clear or (clear and clear->empty())) {
        context_entries.clear();
      } else if (clear) {
        auto clear_v = false;
        if (not parsers::boolean(*clear, clear_v)) {
          return caf::make_error(ec::invalid_argument,
                                 "value for 'clear' key needs to be a valid "
                                 "boolean <true/false>");
        }
        if (clear_v) {
          context_entries.clear();
        }
      }
    }
    TENZIR_ASSERT(slice.rows() != 0);
    if (not parameters.contains("key")) {
      return caf::make_error(ec::invalid_argument, "missing 'key' parameter");
    }
    auto key_field = parameters["key"];
    if (not key_field) {
      return caf::make_error(ec::invalid_argument,
                             "invalid 'key' parameter; 'key' must be a string");
    }
    auto key_column = slice.schema().resolve_key_or_concept(*key_field);
    if (not key_column) {
      // If there's no key column then we cannot do much.
      return update_result{record{}};
    }
    auto [key_type, key_array] = key_column->get(slice);
    auto context_array = std::static_pointer_cast<arrow::Array>(
      to_record_batch(slice)->ToStructArray().ValueOrDie());
    auto key_values = values(key_type, *key_array);
    auto key_values_list = list{};
    auto context_values = values(slice.schema(), *context_array);
    auto key_it = key_values.begin();
    auto context_it = context_values.begin();
    while (key_it != key_values.end()) {
      TENZIR_ASSERT(context_it != context_values.end());
      auto materialized_key = materialize(*key_it);
      context_entries.insert_or_assign(materialized_key,
                                       materialize(*context_it));
      key_values_list.emplace_back(materialized_key);
      ++key_it;
      ++context_it;
    }
    TENZIR_ASSERT(context_it == context_values.end());
    auto query_f = [key_values_list = std::move(key_values_list)](
                     parameter_map params) -> caf::expected<expression> {
      auto column = params["field"];
      return expression{
        predicate{
          field_extractor(*column),
          relational_operator::in,
          data{key_values_list},
        },
      };
    };
    return update_result{.update_info = show(),
                         .make_query = std::move(query_f)};
  }

  auto make_query() -> make_query_type override {
    auto key_values_list = list{};
    key_values_list.reserve(context_entries.size());
    for (const auto& entry : context_entries) {
      key_values_list.emplace_back(entry.first);
    }
    return [key_values_list = std::move(key_values_list)](
             parameter_map params) -> caf::expected<expression> {
      auto column = params["field"];
      if (not column) {
        return caf::make_error(ec::invalid_argument,
                               "missing 'field' parameter for lookup in "
                               "lookup-table");
      }
      return expression{
        predicate{
          field_extractor(*column),
          relational_operator::in,
          data{key_values_list},
        },
      };
    };
  }

  auto reset(context::parameter_map) -> caf::expected<record> override {
    context_entries.clear();
    return show();
  }

  auto save() const -> caf::expected<chunk_ptr> override {
    // We save the context by formatting into a record of this format:
    //   [{key: key, value: value}, ...]
    auto builder = flatbuffers::FlatBufferBuilder{};
    auto value_offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    value_offsets.reserve(context_entries.size());
    for (const auto& [key, value] : context_entries) {
      auto field_offsets
        = std::vector<flatbuffers::Offset<fbs::data::RecordField>>{};
      field_offsets.reserve(2);
      const auto key_key_offset = builder.CreateSharedString("key");
      const auto key_value_offset = pack(builder, key);
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, key_key_offset, key_value_offset));
      const auto value_key_offset = builder.CreateSharedString("value");
      const auto value_value_offset = pack(builder, value);
      field_offsets.emplace_back(fbs::data::CreateRecordField(
        builder, value_key_offset, value_value_offset));
      const auto record_offset
        = fbs::data::CreateRecordDirect(builder, &field_offsets);
      value_offsets.emplace_back(fbs::CreateData(
        builder, fbs::data::Data::record, record_offset.Union()));
    }
    const auto list_offset
      = fbs::data::CreateListDirect(builder, &value_offsets);
    const auto data_offset
      = fbs::CreateData(builder, fbs::data::Data::list, list_offset.Union());
    fbs::FinishDataBuffer(builder, data_offset);
    return chunk::make(builder.Release());
  }

private:
  tsl::robin_map<data, data> context_entries;
  std::optional<generator<table_slice>> entry_dumper;
};

class plugin : public virtual context_plugin {
  auto initialize(const record&, const record&) -> caf::error override {
    return caf::none;
  }

  auto name() const -> std::string override {
    return "lookup-table";
  }

  auto make_context(context::parameter_map) const
    -> caf::expected<std::unique_ptr<context>> override {
    return std::make_unique<ctx>();
  }

  auto load_context(chunk_ptr serialized) const
    -> caf::expected<std::unique_ptr<context>> override {
    auto fb = flatbuffer<fbs::Data>::make(std::move(serialized));
    if (not fb) {
      return caf::make_error(ec::serialization_error,
                             fmt::format("failed to deserialize lookup table "
                                         "context: {}",
                                         fb.error()));
    }
    auto context_entries = tsl::robin_map<data, data>{};
    const auto* list = fb.value()->data_as_list();
    if (not list) {
      return caf::make_error(ec::serialization_error,
                             "failed to deserialize lookup table "
                             "context: no valid list value for "
                             "serialized context entry list");
    }
    if (const auto* list = fb.value()->data_as_list()) {
      if (not list->values()) {
        return caf::make_error(ec::serialization_error,
                               "failed to deserialize lookup table "
                               "context: missing or invalid values for "
                               "context entry in serialized entry list");
      }
      for (const auto* list_value : *list->values()) {
        const auto* record = list_value->data_as_record();
        if (not record) {
          return caf::make_error(ec::serialization_error,
                                 "failed to deserialize lookup table "
                                 "context: invalid type for "
                                 "context entry in serialized entry list, "
                                 "entry must be a record");
        }
        if (not record->fields() or record->fields()->size() != 2) {
          return caf::make_error(ec::serialization_error,
                                 "failed to deserialize lookup table "
                                 "context: invalid or missing value for "
                                 "context entry in serialized entry list, "
                                 "entry must be a record {key, value}");
        }
        data key;
        data value;
        auto err = unpack(*record->fields()->Get(0)->data(), key);
        if (err) {
          return caf::make_error(ec::serialization_error,
                                 fmt::format("failed to deserialize lookup "
                                             "table "
                                             "context: invalid key: {}",
                                             err));
        }
        err = unpack(*record->fields()->Get(1)->data(), value);
        if (err) {
          return caf::make_error(ec::serialization_error,
                                 fmt::format("failed to deserialize lookup "
                                             "table "
                                             "context: invalid value: {}",
                                             err));
        }
        context_entries.emplace(std::move(key), std::move(value));
      }
    }

    return std::make_unique<ctx>(std::move(context_entries));
  }
};

} // namespace

} // namespace tenzir::plugins::lookup_table

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lookup_table::plugin)
