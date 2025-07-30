//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/collect.hpp"
#include "tenzir/detail/enum.hpp"
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

TENZIR_ENUM(
  transfer_metadata_strategy,
  /* preserve the metadata of the old series */
  preserve,
  /* use the metadata of the replacement series */
  replace,
  /* merge the metadata of old and new, using the old entries on conflict */
  merge_preserve,
  /* merge the metadata of old and new, using the new entries on conflict */
  merge_replace);

inline auto transfer_metadata(const type& old, type replacement,
                              transfer_metadata_strategy metadata) -> type {
  switch (metadata) {
    using enum transfer_metadata_strategy;
    case preserve: {
      replacement.assign_metadata(old);
      return replacement;
    }
    case replace: {
      return replacement;
    }
    case merge_preserve: {
      auto new_meta = collect(old.attributes());
      for (auto [k, v] : replacement.attributes()) {
        const auto it
          = std::ranges::find(new_meta, k, &type::attribute_view::key);
        if (it == new_meta.end()) {
          new_meta.emplace_back(k, v);
        }
      }
      return type{replacement, std::move(new_meta)};
    }
    case merge_replace: {
      auto new_meta = collect(old.attributes());
      for (auto [k, v] : replacement.attributes()) {
        const auto it
          = std::ranges::find(new_meta, k, &type::attribute_view::key);
        if (it == new_meta.end()) {
          new_meta.emplace_back(k, v);
        } else {
          it->value = v;
        }
      }
      return type{replacement, std::move(new_meta)};
    }
  }
  TENZIR_UNREACHABLE();
}

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
  transfer_metadata_strategy metadata = transfer_metadata_strategy::preserve;

  auto operator()(const series& s) -> std::optional<series> {
    if constexpr (replacer_for_erased_series<F>) {
      if (auto outer_transformed = transform(s)) {
        return *outer_transformed;
      }
    }
    if constexpr (replacer_for_typed_series<F>) {
      if (auto nested_transformed = match(s, *this)) {
        nested_transformed->type = transfer_metadata(
          s.type, std::move(nested_transformed->type), metadata);
        return *nested_transformed;
      }
    }
    return std::nullopt;
  }

  template <typename T>
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
    for (auto i = uint32_t{}; i < static_cast<uint32_t>(fields.size()); ++i) {
      fields[i].data.array = r.array->field(detail::narrow_cast<int>(i));
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
auto replace(basic_series<T> s, F transform,
             transfer_metadata_strategy metadata
             = transfer_metadata_strategy::preserve)
  -> std::pair<bool, series> {
  auto transformer = replace_visitor{std::move(transform), metadata};
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
auto replace(const table_slice& slice, F transform,
             transfer_metadata_strategy metadata
             = transfer_metadata_strategy::preserve)
  -> std::pair<bool, table_slice> {
  const auto& input_type = as<record_type>(slice.schema());
  auto rb = to_record_batch(slice);
  auto input_struct_array = check(rb->ToStructArray());
  auto transformer = replace_visitor{std::move(transform), metadata};
  auto transformed = transformer(series{input_type, input_struct_array});
  if (not transformed) {
    return {false, slice};
  }
  auto attrs = std::vector<tenzir::type::attribute_view>{};
  auto transformed_type = transformed->type;
  transformed_type.assign_metadata(slice.schema());
  auto& transformed_array = as<arrow::StructArray>(*transformed->array);
  auto output_batch
    = arrow::RecordBatch::Make(transformed_type.to_arrow_schema(),
                               transformed_array.length(),
                               transformed_array.fields());
  auto result = table_slice{output_batch, std::move(transformed_type)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  return {true, result};
}

} // namespace tenzir
