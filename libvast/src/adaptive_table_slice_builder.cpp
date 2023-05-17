//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"

#include <arrow/record_batch.h>

namespace vast {

namespace {
auto init_root_builder(const type& start_schema, bool allow_fields_discovery)
  -> std::variant<detail::concrete_series_builder<record_type>,
                  detail::fixed_fields_record_builder> {
  VAST_ASSERT(caf::holds_alternative<record_type>(start_schema));
  if (allow_fields_discovery)
    return detail::concrete_series_builder<record_type>{
      caf::get<record_type>(start_schema)};
  return detail::fixed_fields_record_builder{
    std::move(caf::get<record_type>(start_schema))};
}
} // namespace

adaptive_table_slice_builder::adaptive_table_slice_builder(
  type start_schema, bool allow_fields_discovery)
  : root_builder_{init_root_builder(start_schema, allow_fields_discovery)} {
}

auto adaptive_table_slice_builder::push_row() -> row_guard {
  return row_guard{*this};
}

auto adaptive_table_slice_builder::finish(std::string_view slice_schema_name)
  -> table_slice {
  auto final_array = finish_impl();
  if (not final_array)
    return table_slice{};
  auto slice_schema = get_schema(slice_schema_name);
  const auto& struct_array
    = static_cast<const arrow::StructArray&>(*final_array);
  const auto batch
    = arrow::RecordBatch::Make(slice_schema.to_arrow_schema(),
                               struct_array.length(), struct_array.fields());
  VAST_ASSERT(batch);
  return table_slice{batch, std::move(slice_schema)};
}

auto adaptive_table_slice_builder::rows() const -> detail::arrow_length_type {
  return std::visit(
    [](const auto& builder) {
      return builder.length();
    },
    root_builder_);
}

auto adaptive_table_slice_builder::get_schema(
  std::string_view slice_schema_name) const -> type {
  return std::visit(
    [slice_schema_name](const auto& builder) -> vast::type {
      auto schema = builder.type();
      if (slice_schema_name.empty())
        return type{schema.make_fingerprint(), schema};
      return type{slice_schema_name, std::move(schema)};
    },
    root_builder_);
}

auto adaptive_table_slice_builder::finish_impl()
  -> std::shared_ptr<arrow::Array> {
  return std::visit(
    [](auto& b) {
      return b.finish();
    },
    root_builder_);
}

adaptive_table_slice_builder::row_guard::row_guard(
  adaptive_table_slice_builder& builder)
  : builder_{builder}, starting_rows_count_{builder_.rows()} {
}

auto adaptive_table_slice_builder::row_guard::cancel() -> void {
  auto current_rows = builder_.rows();
  if (auto row_added = current_rows > starting_rows_count_; row_added) {
    std::visit(
      [](auto& b) {
        b.remove_last_row();
      },
      builder_.root_builder_);
  }
}

auto adaptive_table_slice_builder::row_guard::push_field(
  std::string_view field_name) -> detail::field_guard {
  auto provider
    = std::visit(detail::overload{
                   [field_name](detail::fixed_fields_record_builder& b) {
                     return b.get_field_builder_provider(field_name);
                   },
                   [field_name, len = starting_rows_count_](
                     detail::concrete_series_builder<record_type>& b) {
                     return b.get_field_builder_provider(field_name, len);
                   },
                 },
                 builder_.root_builder_);

  return {std::move(provider), starting_rows_count_};
}

adaptive_table_slice_builder::row_guard::~row_guard() noexcept {
  std::visit(
    [](auto& b) {
      b.fill_nulls();
    },
    builder_.root_builder_);
}

} // namespace vast
