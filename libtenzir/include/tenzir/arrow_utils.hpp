//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/type_traits.h>

#include <source_location>

namespace tenzir {

inline void
check(const arrow::Status& status, std::source_location location
                                   = std::source_location::current()) {
  if (not status.ok()) [[unlikely]] {
    panic_at(location, "{}", status.ToString());
  }
}

template <class T>
[[nodiscard]] auto
check(arrow::Result<T> result, std::source_location location
                               = std::source_location::current()) -> T {
  check(result.status(), location);
  return result.MoveValueUnsafe();
}

template <std::derived_from<arrow::ArrayBuilder> T>
[[nodiscard]] auto
finish(T& x, std::source_location location = std::source_location::current()) {
  auto array = check(x.Finish(), location);
  using concrete_array_type = type_to_arrow_array_t<type_from_arrow_t<T>>;
  /// Using a `dynamic_pointer_cast` here sometimes fails on using MacOS llvm.
  /// A `static_cast` is save here, because we statically know the concrete type
  /// from the builder.
  return std::static_pointer_cast<concrete_array_type>(array);
}

auto data_to_series(const data& value, int64_t length) -> series;
auto data_to_series(const data& value, uint64_t length) -> series;

// -- column builder helpers --------------------------------------------------

arrow::Status
append_builder(const null_type&, type_to_arrow_builder_t<null_type>& builder,
               const view<type_to_data_t<null_type>>& view) noexcept;

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept;

arrow::Status
append_builder(const int64_type&, type_to_arrow_builder_t<int64_type>& builder,
               const view<type_to_data_t<int64_type>>& view) noexcept;

arrow::Status
append_builder(const uint64_type&,
               type_to_arrow_builder_t<uint64_type>& builder,
               const view<type_to_data_t<uint64_type>>& view) noexcept;

arrow::Status
append_builder(const double_type&,
               type_to_arrow_builder_t<double_type>& builder,
               const view<type_to_data_t<double_type>>& view) noexcept;

arrow::Status
append_builder(const duration_type&,
               type_to_arrow_builder_t<duration_type>& builder,
               const view<type_to_data_t<duration_type>>& view) noexcept;

arrow::Status
append_builder(const time_type&, type_to_arrow_builder_t<time_type>& builder,
               const view<type_to_data_t<time_type>>& view) noexcept;

arrow::Status
append_builder(const string_type&,
               type_to_arrow_builder_t<string_type>& builder,
               const view<type_to_data_t<string_type>>& view) noexcept;

arrow::Status
append_builder(const blob_type&, type_to_arrow_builder_t<blob_type>& builder,
               const view<type_to_data_t<blob_type>>& view) noexcept;

arrow::Status
append_builder(const ip_type&, type_to_arrow_builder_t<ip_type>& builder,
               const view<type_to_data_t<ip_type>>& view) noexcept;

arrow::Status
append_builder(const subnet_type&,
               type_to_arrow_builder_t<subnet_type>& builder,
               const view<type_to_data_t<subnet_type>>& view) noexcept;

arrow::Status
append_builder(const enumeration_type&,
               type_to_arrow_builder_t<enumeration_type>& builder,
               const view<type_to_data_t<enumeration_type>>& view) noexcept;

arrow::Status
append_builder(const list_type& hint,
               type_to_arrow_builder_t<list_type>& builder,
               const view<type_to_data_t<list_type>>& view) noexcept;

arrow::Status
append_builder(const map_type& hint, type_to_arrow_builder_t<map_type>& builder,
               const view<type_to_data_t<map_type>>& view) noexcept;

arrow::Status
append_builder(const record_type& hint,
               type_to_arrow_builder_t<record_type>& builder,
               const view<type_to_data_t<record_type>>& view) noexcept;

arrow::Status
append_builder(const secret_type& hint,
               type_to_arrow_builder_t<secret_type>& builder,
               const view<type_to_data_t<secret_type>>& view) noexcept;

template <type_or_concrete_type Type>
arrow::Status
append_builder(const Type& hint,
               std::same_as<arrow::ArrayBuilder> auto& builder,
               const std::same_as<data_view> auto& view) noexcept {
  if (is<caf::none_t>(view)) {
    return builder.AppendNull();
  }
  if constexpr (concrete_type<Type>) {
    return append_builder(hint, as<type_to_arrow_builder_t<Type>>(builder),
                          as<tenzir::view<type_to_data_t<Type>>>(view));
  } else {
    auto f
      = [&]<concrete_type ResolvedType>(const ResolvedType& hint) noexcept {
          return append_builder(
            hint, as<type_to_arrow_builder_t<ResolvedType>>(builder),
            as<tenzir::view<type_to_data_t<ResolvedType>>>(view));
        };
    return match(hint, f);
  }
}

auto append_array_slice(arrow::ArrayBuilder& builder, const type& ty,
                        const arrow::Array& array, int64_t begin, int64_t count)
  -> arrow::Status;

template <concrete_type Ty>
auto append_array_slice(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                        const type_to_arrow_array_t<Ty>& array, int64_t begin,
                        int64_t count) -> arrow::Status;

template <type_or_concrete_type Ty>
auto append_array(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                  const type_to_arrow_array_t<Ty>& array) -> arrow::Status {
  return append_array_slice(builder, ty, array, 0, array.length());
}

} // namespace tenzir
