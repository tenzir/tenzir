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
#include <tenzir/fbs/data.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/project.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/typed_array.hpp>

#include <arrow/array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>
#include <caf/sum_type.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <memory>
#include <string>

namespace tenzir::plugins::hashtable_context {

namespace {

class ctx final : public virtual context {
public:
  ctx() noexcept = default;

  explicit ctx(tsl::robin_map<data, data> context_entries) noexcept
    : context_entries{std::move(context_entries)} {
    // nop
  }

  /// Emits context information for every event in `slice` in order.
  auto apply(table_slice slice, record parameters) const
    -> caf::expected<std::vector<typed_array>> override {
    auto resolved_slice = resolve_enumerations(slice);
    auto field_name = std::optional<std::string>{};
    for (const auto& [key, value] : parameters) {
      if (key == "field") {
        const auto* str = caf::get_if<std::string>(&value);
        if (not str) {
          return caf::make_error(ec::invalid_argument,
                                 "invalid argument type for `field`: expected "
                                 "a string");
        }
        field_name = *str;
        continue;
      }
      return caf::make_error(ec::invalid_argument,
                             fmt::format("invalid argument `{}`", key));
    }
    if (not field_name) {
      return caf::make_error(ec::invalid_argument, "missing argument `field`");
    }
    auto field_builder = series_builder{};
    auto column_offset
      = caf::get<record_type>(slice.schema()).resolve_key(*field_name);
    if (not column_offset) {
      for (auto i = size_t{0}; i < slice.rows(); ++i) {
        field_builder.null();
      }
      return field_builder.finish();
    }
    auto [type, slice_array] = column_offset->get(resolved_slice);
    for (auto value : values(type, *slice_array)) {
      if (auto it = context_entries.find(value); it != context_entries.end()) {
        auto r = field_builder.record();
        r.field("key", it->first);
        r.field("context", it->second);
        r.field("timestamp", std::chrono::system_clock::now());
      } else {
        field_builder.null();
      }
    }
    return field_builder.finish();
  }

  /// Inspects the context.
  auto show() const -> record override {
    auto entries = list{};
    for (const auto& [key, context] : context_entries) {
      entries.push_back(record{
        {"key", key},
        {"context", context},
      });
    }
    return record{
      {"num_entries", context_entries.size()},
      {"entries", std::move(entries)}, // TODO: is this too verbose?
    };
  }

  /// Updates the context.
  auto update(table_slice slice, record parameters)
    -> caf::expected<record> override {
    // context does stuff on its own with slice & parameters
    if (parameters.contains("clear")) {
      auto* clear = get_if<bool>(&parameters, "clear");
      if (clear and *clear) {
        context_entries.clear();
      }
    }
    if (slice.rows() == 0) {
      // We can ignore empty slices.
      return record{};
    }
    const auto& layout = caf::get<record_type>(slice.schema());
    auto key_column = layout.resolve_key("key");
    if (not key_column) {
      // If there's no key column then we cannot do much.
      return record{};
    }
    auto [key_type, key_array] = key_column->get(slice);
    auto context_column = layout.resolve_key("context");
    auto context_type = type{};
    auto context_array = std::static_pointer_cast<arrow::Array>(
      std::make_shared<arrow::NullArray>(slice.rows()));
    if (context_column) {
      std::tie(context_type, context_array) = context_column->get(slice);
    }
    auto key_values = values(key_type, *key_array);
    auto context_values = values(context_type, *context_array);
    auto key_it = key_values.begin();
    auto context_it = context_values.begin();
    while (key_it != key_values.end()) {
      TENZIR_ASSERT_CHEAP(context_it != context_values.end());
      context_entries.emplace(materialize(*key_it), materialize(*context_it));
      ++key_it;
      ++context_it;
    }
    TENZIR_ASSERT_CHEAP(context_it == context_values.end());
    return record{{"updated", slice.rows()}};
  }

  auto update(chunk_ptr bytes, record parameters)
    -> caf::expected<record> override {
    return ec::unimplemented;
  }

  auto update(record parameters) -> caf::expected<record> override {
    return ec::unimplemented;
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
    auto fb = flatbuffer<fbs::Data>::make(std::move(serialized));
    if (not fb) {
      return caf::make_error(
        ec::serialization_error,
        fmt::format("failed to deserialize hashtbale context: {}", fb.error()));
    }
    // FIXME: reconstruct context_entries from fb
    auto context_entries = tsl::robin_map<data, data>{};
    return std::make_unique<ctx>(std::move(context_entries));
  }
};

} // namespace

} // namespace tenzir::plugins::hashtable_context

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hashtable_context::plugin)
