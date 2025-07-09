//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/series.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/variant_traits.hpp"

#include <optional>

namespace tenzir {

template <typename F>
concept replacer_for_erased_series = requires(F f, series s) {
  { f(s) } -> std::same_as<std::optional<series>>;
};

template <typename F>
concept replacer_for_typed_series
  = detail::variant_matcher_for<F, series>
    and requires(F f, basic_series<null_type> s) {
          { f(s) } -> std::same_as<std::optional<series>>;
        };

/// Applies `transform` to all columns in a slice when called.
/// * If `transform(series)` is possible, this will be called before matching.
///   If it produces a transform result, the result is used/returned directly.
/// * If `transform` is a valid matcher for `series` (callable with all
///   `basic_series` instantiations), those will be used next. If those produce
///   a transform result, metadata from the original will be copied over.
template <typename F>
  requires(replacer_for_typed_series<F> or replacer_for_erased_series<F>)
struct replace_visitor {
  F transform;

  auto operator()(const series& s) -> std::optional<series> {
    if constexpr (replacer_for_erased_series<F>) {
      if (auto outer_transformed = transform(s)) {
        return *outer_transformed;
      }
    }
    if constexpr (replacer_for_typed_series<F>) {
      if (auto nested_transformed = match(s, *this)) {
        series& res = *nested_transformed;
        res.type.assign_metadata(s.type);
        return res;
      }
    }
    return std::nullopt;
  }

  template <concepts::none_of<tenzir::type> T>
  auto operator()(const basic_series<T>& s) -> std::optional<series> {
    return transform(s);
  }

  auto operator()(const basic_series<list_type>& l) -> std::optional<series> {
    auto nested_replacement
      = (*this)(series{l.type.value_type(), l.array->values()});
    if (not nested_replacement) {
      return std::nullopt;
    }
    return make_list_series(*nested_replacement, *l.array);
  }

  auto operator()(const basic_series<record_type>& r) -> std::optional<series> {
    auto fields = std::vector<series_field>{};
    fields.reserve(r.type.num_fields());
    for (const auto& f : r.type.fields()) {
      fields.emplace_back(f.name, series{f.type, nullptr});
    }
    TENZIR_ASSERT(static_cast<size_t>(r.type.num_fields()) == fields.size(),
                  "{} != {}", r.type.num_fields(), fields.size());
    TENZIR_ASSERT(r.type.num_fields()
                  == static_cast<size_t>(r.array->num_fields()));
    for (auto i = 0l; i < static_cast<uint32_t>(fields.size()); ++i) {
      fields[i].data.array = r.array->field(i);
    }
    auto any_replacement = false;
    for (auto& field : fields) {
      auto nested_replacement = (*this)(field.data);
      if (not nested_replacement) {
        continue;
      }
      any_replacement = true;
      TENZIR_ASSERT(field.data.length() == nested_replacement->array->length());
      field.data = std::move(*nested_replacement);
    }
    if (not any_replacement) {
      return std::nullopt;
    }
    return make_record_series(std::move(fields), *r.array);
  }
};

/// Recurses over all columns in `slice`, potentially replacing them with the
/// result of `transform(column)`.
/// Unlike `tenzir::transform_columns`, this does enter into lists, invoking the
/// transform on both the list itself and its data array.
template <typename F, typename T>
  requires(replacer_for_erased_series<F> or replacer_for_typed_series<F>)
auto replace(basic_series<T> s, F transform) -> std::pair<bool, series> {
  auto transformer = replace_visitor{std::move(transform)};
  auto transformed = transformer(s);
  if (not transformed) {
    return {false, s};
  }
  return {true, *transformed};
}

/// Recurses over all columns in `slice`, potentially replacing them with the
/// result of `transform(column)`.
/// Unlike `tenzir::transform_columns`, this does enter into lists, invoking the
/// transform on both the list itself and its data array.
/// @relates `replace_visitor`
template <typename F>
  requires(replacer_for_erased_series<F> or replacer_for_typed_series<F>)
auto replace(table_slice slice, F transform) -> std::pair<bool, table_slice> {
  const auto& input_type = as<record_type>(slice.schema());
  auto rb = to_record_batch(slice);
  auto input_struct_array = check(rb->ToStructArray());
  auto transformer = replace_visitor{std::move(transform)};
  auto transformed = transformer(series{input_type, input_struct_array});
  if (not transformed) {
    return {false, slice};
  }
  auto& transformed_type = transformed->type;
  transformed_type.assign_metadata(slice.schema());
  auto& transformed_array = as<arrow::StructArray>(*transformed->array);
  auto output_batch
    = arrow::RecordBatch::Make(transformed_type.to_arrow_schema(),
                               transformed_array.length(),
                               transformed_array.fields());
  auto result = table_slice{output_batch, std::move(transformed->type)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return {true, result};
}

} // namespace tenzir
