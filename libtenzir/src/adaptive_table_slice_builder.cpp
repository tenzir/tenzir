//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_table_slice_builder.hpp"

#include <arrow/record_batch.h>

#include <memory>
#include <utility>

namespace tenzir {

namespace {
auto init_root_builder(const type& start_schema, bool allow_fields_discovery)
  -> detail::adaptive_builder_root {
  TENZIR_ASSERT(caf::holds_alternative<record_type>(start_schema));
  if (allow_fields_discovery) {
    return detail::adaptive_builder_root{
      std::in_place_type<detail::concrete_series_builder<record_type>>,
      caf::get<record_type>(start_schema)};
  }
  return detail::adaptive_builder_root{
    std::in_place_type<detail::fixed_fields_record_builder>,
    std::move(caf::get<record_type>(start_schema))};
}
} // namespace

adaptive_table_slice_builder::adaptive_table_slice_builder(
  type start_schema, bool allow_fields_discovery)
  // Note: We rely on move-elision here, but this is quite brittle.
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
  TENZIR_ASSERT(batch);
  auto ret = table_slice{batch, std::move(slice_schema)};
  ret.offset(0u);
  return ret;
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
    [slice_schema_name](const auto& builder) -> tenzir::type {
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
        b.fill_nulls();
        b.remove_last_row();
      },
      builder_.root_builder_);
  }
}

auto adaptive_table_slice_builder::row_guard::push_field(
  std::string_view field_name) -> detail::field_guard {
  auto [builder_provider, parent_record_builder_provider] = std::visit(
    detail::overload{
      [field_name](detail::fixed_fields_record_builder& b)
        -> std::pair<detail::builder_provider,
                     detail::parent_record_builder_provider> {
        return std::pair{b.get_field_builder_provider(field_name), []() {
                           return nullptr;
                         }};
      },
      [field_name, len = starting_rows_count_](
        detail::concrete_series_builder<record_type>& b)
        -> std::pair<detail::builder_provider,
                     detail::parent_record_builder_provider> {
        return std::pair{b.get_field_builder_provider(field_name, len), [&b]() {
                           return std::addressof(b);
                         }};
      },
    },
    builder_.root_builder_);

  return {std::move(builder_provider),
          std::move(parent_record_builder_provider), starting_rows_count_};
}

adaptive_table_slice_builder::row_guard::~row_guard() noexcept {
  std::visit(
    [](auto& b) {
      b.fill_nulls();
    },
    builder_.root_builder_);
}

void adaptive_table_slice_builder::reset() {
  root_builder_.emplace<0>();
}

} // namespace tenzir
