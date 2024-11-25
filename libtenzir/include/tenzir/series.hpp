//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/type.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <caf/detail/is_one_of.hpp>

#include <memory>

namespace tenzir {

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
      array{to_record_batch(slice)->ToStructArray().ValueOrDie()} {
  }

  explicit basic_series(const table_slice& slice)
    requires(std::same_as<Type, record_type>)
    : type{as<record_type>(slice.schema())},
      array{to_record_batch(slice)->ToStructArray().ValueOrDie()} {
  }

  basic_series(table_slice slice, offset idx)
    requires(std::same_as<Type, type>)
  {
    std::tie(type, array) = idx.get(slice);
  }

  template <class Other>
    requires(std::same_as<Type, type> || std::same_as<Other, Type>)
  basic_series(Other type, std::shared_ptr<type_to_arrow_array_t<Type>> array)
    : type{std::move(type)}, array{std::move(array)} {
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

  auto length() const -> int64_t {
    return array ? array->length() : 0;
  }

  template <type_or_concrete_type Other>
    requires(std::same_as<Type, type> || std::same_as<Other, Type>)
  static auto null(Other ty, int64_t length) -> basic_series<Type> {
    auto b = ty.make_arrow_builder(arrow::default_memory_pool());
    // TODO
    (void)b->AppendNulls(length);
    return {std::move(ty),
            std::static_pointer_cast<type_to_arrow_array_t<Other>>(
              b->Finish().ValueOrDie())};
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
    auto sliced = array->SliceSafe(begin, end - begin);
    TENZIR_ASSERT(sliced.ok());
    return {type, sliced.MoveValueUnsafe()};
  }

  Type type;
  std::shared_ptr<type_to_arrow_array_t<Type>> array;
};

/// A series represents a contiguous representation of nullable data of the same
/// type, e.g., a column in a table slice.
using series = basic_series<type>;

} // namespace tenzir
