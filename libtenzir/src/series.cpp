//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/try.hpp"

#include <tenzir/series.hpp>

namespace tenzir {

/// Flattens a `series` if it is a record, returning it as-is otherwise
auto flatten(series s, std::string_view flatten_separator)
  -> flatten_series_result {
  if (not s.type.kind().is<record_type>()) {
    return {std::move(s), {}};
  }
  auto [t, arr, renames]
    = flatten(s.type, std::dynamic_pointer_cast<arrow::StructArray>(s.array),
              flatten_separator);
  return {series{std::move(t), std::move(arr)}, std::move(renames)};
}

template <class Type>
auto basic_series<Type>::field(std::string_view name) const
  -> std::optional<series>
  requires(std::same_as<Type, record_type>)
{
  TRY(auto index, type.resolve_field(name));
  return series{type.field(index).type,
                array->field(detail::narrow<int>(index))};
}

template <class Type>
auto basic_series<Type>::fields() const -> generator<series_field>
  requires(std::same_as<Type, record_type>)
{
  for (auto [index, field] : detail::enumerate<int>(type.fields())) {
    co_yield series_field{field.name, series{field.type, array->field(index)}};
  }
}

template struct basic_series<record_type>;

auto make_record_series(std::span<const series_field> fields,
                        const arrow::StructArray& origin)
  -> basic_series<record_type> {
  auto tenzir_fields = std::vector<record_type::field_view>{};
  auto arrow_fields = arrow::FieldVector{};
  auto children = arrow::ArrayVector{};
  for (auto& field : fields) {
    TENZIR_ASSERT(field.data.length() == origin.length());
    tenzir_fields.emplace_back(field.name, field.data.type);
    arrow_fields.push_back(field.data.type.to_arrow_field(field.name));
    children.push_back(field.data.array);
  }
  auto null_bitmap = origin.null_bitmap();
  if (origin.offset() != 0 and origin.null_bitmap_data()) {
    null_bitmap
      = check(arrow::internal::CopyBitmap(arrow_memory_pool(),
                                          origin.null_bitmap_data(),
                                          origin.offset(), origin.length()));
  }
  return {
    record_type{tenzir_fields},
    std::make_shared<arrow::StructArray>(
      arrow::struct_(arrow_fields), origin.length(), children,
      std::move(null_bitmap), origin.data()->null_count, 0),
  };
}

auto make_list_series(const series& values, const arrow::ListArray& origin)
  -> basic_series<list_type> {
  return {
    list_type{values.type},
    std::make_shared<arrow::ListArray>(
      arrow::list(values.type.to_arrow_type()), origin.length(),
      origin.value_offsets(), values.array, origin.null_bitmap(),
      origin.data()->null_count, origin.offset()),
  };
}

} // namespace tenzir
