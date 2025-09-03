//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <memory>

namespace tenzir {

struct series_field;

template <class Type>
struct basic_series {
  basic_series() = default;

  template <concrete_type Other>
    requires(std::same_as<Type, type>)
  explicit(false) basic_series(basic_series<Other> other)
    : type{other.type}, array{other.array} {
  }

  explicit basic_series(const table_slice& slice)
    requires(std::same_as<Type, type>)
    : type{slice.schema()},
      array{check(to_record_batch(slice)->ToStructArray())} {
  }

  explicit basic_series(const table_slice& slice)
    requires(std::same_as<Type, record_type>)
    : type{tenzir::as<record_type>(slice.schema())},
      array{check(to_record_batch(slice)->ToStructArray())} {
  }

  basic_series(table_slice slice, offset idx)
    requires(std::same_as<Type, type>)
  {
    std::tie(type, array) = idx.get(slice);
  }

  /// Type + Type::Array -> basic_series<Type>
  basic_series(Type type, std::shared_ptr<type_to_arrow_array_t<Type>> array)
    : type{std::move(type)}, array{std::move(array)} {
    TENZIR_ASSERT(this->array);
    TENZIR_ASSERT_EXPENSIVE(this->type.to_arrow_type()->id()
                            == this->array->type_id());
  }

  /// concrete type + concrete array -> erased series
  template <concrete_type Other>
    requires(std::same_as<Type, tenzir::type>)
  basic_series(Other type, std::shared_ptr<type_to_arrow_array_t<Other>> array)
    : type{tenzir::type{std::move(type)}}, array{std::move(array)} {
    TENZIR_ASSERT(this->array);
    TENZIR_ASSERT_EXPENSIVE(this->type.to_arrow_type()->id()
                            == this->array->type_id());
  }

  /// concrete type + erased array -> erased series
  template <concrete_type Other>
    requires(std::same_as<Type, tenzir::type>)
  basic_series(Other type, std::shared_ptr<arrow::Array> array)
    : type{tenzir::type{std::move(type)}}, array{std::move(array)} {
    TENZIR_ASSERT(this->array);
    TENZIR_ASSERT_EXPENSIVE(this->type.to_arrow_type()->id()
                            == this->array->type_id());
  }

  explicit(false)
    basic_series(std::shared_ptr<type_to_arrow_array_t<Type>> array)
    requires basic_type<Type>
    : type{Type{}}, array{std::move(array)} {
    TENZIR_ASSERT(this->array);
  }

  // TODO: std::get_if, etc.
  template <type_or_concrete_type Other>
    requires(std::same_as<Type, type>)
  auto as() const -> std::optional<basic_series<Other>> {
    if constexpr (std::same_as<Other, tenzir::type>) {
      return *this;
    } else {
      auto other_type = try_as<Other>(&type);
      if (not other_type) {
        return std::nullopt;
      }
      // TODO: This could also be a `dynamic_cast`, but that is sometimes broken
      // when calling this from plugins (due to duplicated RTTI information).
      TENZIR_ASSERT(typeid(*array) == typeid(type_to_arrow_array_t<Other>));
      auto other_array
        = std::static_pointer_cast<type_to_arrow_array_t<Other>>(array);
      return basic_series<Other>{*other_type, std::move(other_array)};
    }
  }

  auto field(std::string_view name) const -> std::optional<series>
    requires(std::same_as<Type, record_type>);

  auto fields() const -> generator<series_field>
    requires(std::same_as<Type, record_type>);

  auto length() const -> int64_t {
    return array ? array->length() : 0;
  }

  template <type_or_concrete_type Other>
    requires(std::same_as<Type, type> || std::same_as<Other, Type>)
  static auto null(Other ty, int64_t length) -> basic_series<Type> {
    auto b = ty.make_arrow_builder(arrow_memory_pool());
    // TODO
    check(b->AppendNulls(length));
    return {std::move(ty),
            std::static_pointer_cast<type_to_arrow_array_t<Other>>(
              check(b->Finish()))};
  }

  static auto null(int64_t length) -> basic_series<Type>
    requires(basic_type<Type>)
  {
    return null(Type{}, length);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, basic_series& x) -> bool {
    if constexpr (Inspector::is_loading) {
      table_slice slice;
      auto callback = [&]() noexcept {
        x.type = ::tenzir::as<record_type>(slice.schema()).field(0).type;
        x.array = to_record_batch(slice)->column(0);
        return true;
      };
      return f.object(x)
        .pretty_name("tenzir.series")
        .on_load(callback)
        .fields(f.field("slice", slice));
    } else {
      auto arrays = std::vector{x.array};
      auto tenzir_schema = tenzir::type{record_type{{"x", x.type}}};
      auto arrow_schema = tenzir_schema.to_arrow_schema();
      auto batch
        = arrow::RecordBatch::Make(arrow_schema, x.array->length(), arrays);
      auto slice
        = table_slice{batch, tenzir_schema, table_slice::serialize::yes};
      return f.object(x)
        .pretty_name("tenzir.series")
        .fields(f.field("slice", slice));
    }
  }

  template <type_or_concrete_type Cast = type>
    requires(std::same_as<Type, type>)
  auto values() const {
    if constexpr (concrete_type<Cast>) {
      const auto* ct = try_as<Cast>(&type);
      TENZIR_ASSERT(ct);
      TENZIR_ASSERT(array);
      return tenzir::values(
        *ct, static_cast<const type_to_arrow_array_t<Cast>&>(*array));
    } else {
      TENZIR_ASSERT(array);
      return tenzir::values(type, *array);
    }
  }

  auto values() const
    requires(concrete_type<Type>)
  {
    TENZIR_ASSERT(array);
    return tenzir::values(type, *array);
  }

  [[nodiscard]] auto slice(int64_t begin, int64_t end) const
    -> basic_series<Type> {
    auto sliced = check(array->SliceSafe(begin, end - begin));
    return {
      type,
      std::static_pointer_cast<type_to_arrow_array_t<Type>>(std::move(sliced)),
    };
  }

  auto at(int64_t i) const& {
    return view_at(*array, i);
  }

  Type type;
  std::shared_ptr<type_to_arrow_array_t<Type>> array;
};

/// A series represents a contiguous representation of nullable data of the same
/// type, e.g., a column in a table slice.
using series = basic_series<type>;

template <>
class variant_traits<series> {
public:
  static constexpr auto count = variant_traits<type>::count;

  static auto index(const series& x) -> size_t {
    return variant_traits<type>::index(x.type);
  }

  template <size_t I>
  static auto get(const series& x) -> decltype(auto) {
    auto ty = variant_traits<type>::get<I>(x.type);
    using Type = decltype(ty);
    // Directly using `typeid(*x.array)` leads to a warning.
    TENZIR_ASSERT(x.array);
    auto& deref = *x.array;
    TENZIR_ASSERT(
      typeid(type_to_arrow_array_t<Type>) == typeid(deref), "`{}` != `{}`",
      caf::detail::pretty_type_name(typeid(type_to_arrow_array_t<Type>)),
      caf::detail::pretty_type_name(typeid(deref)));
    auto array = std::static_pointer_cast<type_to_arrow_array_t<Type>>(x.array);
    return basic_series<Type>{std::move(ty), std::move(array)};
  }
};

struct series_field {
  std::string_view name;
  series data;
};

auto make_record_series(std::span<const series_field> fields,
                        const arrow::StructArray& origin)
  -> basic_series<record_type>;

/// Returns a list series with the given inner values, and the list structure
/// derived from an existing `arrow::ListArray`.
auto make_list_series(const series& values, const arrow::ListArray& origin)
  -> basic_series<list_type>;

/// @related flatten
struct flatten_series_result {
  basic_series<type> series;
  std::vector<std::string> renamed_fields = {};
};

/// Flattens a `series` if it is a record, returning it as-is otherwise
/// @param s a series to flatten
/// @param flatten_separator the separator to use in between nested keys
auto flatten(series s, std::string_view flatten_separator = ".")
  -> flatten_series_result;

} // namespace tenzir
