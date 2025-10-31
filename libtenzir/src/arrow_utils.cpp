//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/try.hpp"

#include <arrow/api.h>

namespace tenzir {

namespace {

auto contains_extension_type(const data& x) -> bool {
  return match(
    x,
    [](const record& x) {
      for (auto& y : x) {
        if (contains_extension_type(y.second)) {
          return true;
        }
      }
      return false;
    },
    [](const list& x) {
      for (auto& y : x) {
        if (contains_extension_type(y)) {
          return true;
        }
      }
      return false;
    },
    []<class T>(const T&) -> bool {
      if constexpr (concepts::one_of<T, map, pattern>) {
        TENZIR_UNREACHABLE();
      } else {
        return extension_type<data_to_type_t<T>>;
      }
    });
}

} // namespace

auto data_to_series(const data& value, int64_t length) -> series {
  if (is<caf::none_t>(value)) {
    return series::null(null_type{}, length);
  }
  TENZIR_ASSERT(length >= 0);
  if (contains_extension_type(value)) {
    // We currently cannot convert extension types to scalars.
    auto b = series_builder{};
    if (length == 0) {
      // Still need to get the correct type.
      b.data(value);
      return b.finish_assert_one_array().slice(0, 0);
    }
    for (auto i = int64_t{0}; i < length; ++i) {
      b.data(value);
    }
    return b.finish_assert_one_array();
  }
  auto b = series_builder{};
  b.data(value);
  auto s = b.finish_assert_one_array();
  return series{
    std::move(s.type),
    check(arrow::MakeArrayFromScalar(*check(s.array->GetScalar(0)), length,
                                     tenzir::arrow_memory_pool())),
  };
}

auto data_to_series(const data& value, uint64_t length) -> series {
  return data_to_series(value, detail::narrow<int64_t>(length));
}

arrow::Status
append_builder(const null_type&, type_to_arrow_builder_t<null_type>& builder,
               const view<type_to_data_t<null_type>>& view) noexcept {
  (void)view;
  return builder.AppendNull();
}

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const int64_type&, type_to_arrow_builder_t<int64_type>& builder,
               const view<type_to_data_t<int64_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const uint64_type&,
               type_to_arrow_builder_t<uint64_type>& builder,
               const view<type_to_data_t<uint64_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const double_type&,
               type_to_arrow_builder_t<double_type>& builder,
               const view<type_to_data_t<double_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const duration_type&,
               type_to_arrow_builder_t<duration_type>& builder,
               const view<type_to_data_t<duration_type>>& view) noexcept {
  return builder.Append(view.count());
}

arrow::Status
append_builder(const time_type&, type_to_arrow_builder_t<time_type>& builder,
               const view<type_to_data_t<time_type>>& view) noexcept {
  return builder.Append(view.time_since_epoch().count());
}

arrow::Status
append_builder(const string_type&,
               type_to_arrow_builder_t<string_type>& builder,
               const view<type_to_data_t<string_type>>& view) noexcept {
  return builder.Append(std::string_view{view.data(), view.size()});
}

arrow::Status
append_builder(const blob_type&, type_to_arrow_builder_t<blob_type>& builder,
               const view<type_to_data_t<blob_type>>& view) noexcept {
  return builder.Append(
    std::string_view{reinterpret_cast<const char*>(view.data()), view.size()});
}

arrow::Status
append_builder(const ip_type&, type_to_arrow_builder_t<ip_type>& builder,
               const view<type_to_data_t<ip_type>>& view) noexcept {
  const auto bytes = as_bytes(view);
  TENZIR_ASSERT_EXPENSIVE(bytes.size() == 16);
  return builder.Append(std::string_view{
    reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

arrow::Status
append_builder(const subnet_type&,
               type_to_arrow_builder_t<subnet_type>& builder,
               const view<type_to_data_t<subnet_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok()) {
    return status;
  }
  if (auto status
      = append_builder(ip_type{}, builder.ip_builder(), view.network());
      !status.ok()) {
    return status;
  }
  return builder.length_builder().Append(view.length());
}

arrow::Status
append_builder(const secret_type&,
               type_to_arrow_builder_t<secret_type>& builder,
               const view<type_to_data_t<secret_type>>& view) noexcept {
  TRY(builder.Append());
  TENZIR_ASSERT(view.buffer.chunk());
  TRY(builder.buffer_builder().Append(
    reinterpret_cast<const char*>(view.buffer.chunk()->data()),
    view.buffer.chunk()->size()));
  return arrow::Status::OK();
}

arrow::Status
append_builder(const enumeration_type&,
               type_to_arrow_builder_t<enumeration_type>& builder,
               const view<type_to_data_t<enumeration_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const list_type& hint,
               type_to_arrow_builder_t<list_type>& builder,
               const view<type_to_data_t<list_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok()) {
    return status;
  }
  auto append_values = [&](const concrete_type auto& value_type) noexcept {
    auto& value_builder = *builder.value_builder();
    for (const auto& value_view : view) {
      if (auto status = append_builder(value_type, value_builder, value_view);
          !status.ok()) {
        return status;
      }
    }
    return arrow::Status::OK();
  };
  return match(hint.value_type(), append_values);
}

arrow::Status
append_builder(const map_type& hint, type_to_arrow_builder_t<map_type>& builder,
               const view<type_to_data_t<map_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok()) {
    return status;
  }
  auto append_values = [&](const concrete_type auto& key_type,
                           const concrete_type auto& item_type) noexcept {
    auto& key_builder = *builder.key_builder();
    auto& item_builder = *builder.item_builder();
    for (const auto& [key_view, item_view] : view) {
      if (auto status = append_builder(key_type, key_builder, key_view);
          !status.ok()) {
        return status;
      }
      if (auto status = append_builder(item_type, item_builder, item_view);
          !status.ok()) {
        return status;
      }
    }
    return arrow::Status::OK();
  };
  return match(std::tuple{hint.key_type(), hint.value_type()}, append_values);
}

arrow::Status
append_builder(const record_type& hint,
               type_to_arrow_builder_t<record_type>& builder,
               const view<type_to_data_t<record_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok()) {
    return status;
  }
  for (int index = 0; const auto& [_, field_type] : hint.fields()) {
    if (auto status = append_builder(field_type, *builder.field_builder(index),
                                     view->at(index).second);
        !status.ok()) {
      return status;
    }
    ++index;
  }
  return arrow::Status::OK();
}

arrow::Status append_builder(const type& hint,
                             std::same_as<arrow::ArrayBuilder> auto& builder,
                             const view<type_to_data_t<type>>& value) noexcept {
  if (is<caf::none_t>(value)) {
    return builder.AppendNull();
  }
  auto f = [&]<concrete_type Type>(const Type& hint) {
    return append_builder(hint, as<type_to_arrow_builder_t<Type>>(builder),
                          as<view<type_to_data_t<Type>>>(value));
  };
  return match(hint, f);
}

template <concrete_type Ty>
auto append_array_slice(type_to_arrow_builder_t<Ty>& builder, const Ty& ty,
                        const type_to_arrow_array_t<Ty>& array, int64_t begin,
                        int64_t count) -> arrow::Status {
  TENZIR_ASSERT(0 <= begin);
  auto end = begin + count;
  TENZIR_ASSERT(end <= array.length());
  TRY(builder.Reserve(count));
  if constexpr (arrow::is_extension_type<type_to_arrow_type_t<Ty>>::value) {
    // TODO: `AppendArraySlice(...)` throws a `std::bad_cast` with extension
    // types (Arrow 13.0.0). Hence, we have to use some custom logic here.
    for (auto row = begin; row < end; ++row) {
      if (array.IsNull(row)) {
        TRY(builder.AppendNull());
      } else {
        TRY(append_builder(ty, builder, value_at(ty, *array.storage(), row)));
      }
    }
  } else if constexpr (std::same_as<Ty, record_type>) {
    TENZIR_ASSERT(detail::narrow<size_t>(builder.num_fields())
                  == ty.num_fields());
    TENZIR_ASSERT(array.num_fields() == builder.num_fields());
    for (auto row = begin; row < end; ++row) {
      TRY(builder.Append(array.IsValid(row)));
    }
    for (auto field = 0; field < builder.num_fields(); ++field) {
      TRY(append_array_slice(*builder.field_builder(field),
                             ty.field(field).type, *array.field(field), begin,
                             count));
    }
  } else if constexpr (std::same_as<Ty, list_type>) {
    for (auto row = begin; row < end; ++row) {
      auto valid = array.IsValid(row);
      TRY(builder.Append(valid));
      if (valid) {
        auto list_begin = array.value_offset(row);
        auto list_end = array.value_offset(row + 1);
        TRY(append_array_slice(*builder.value_builder(), ty.value_type(),
                               *array.values(), list_begin,
                               list_end - list_begin));
      }
    }
  } else if constexpr (std::same_as<Ty, map_type>) {
    TENZIR_UNREACHABLE();
  } else {
    static_assert(basic_type<Ty>);
    TRY(builder.AppendArraySlice(*array.data(), begin, count));
  }
  return arrow::Status::OK();
}

auto append_array_slice(arrow::ArrayBuilder& builder, const type& ty,
                        const arrow::Array& array, int64_t begin, int64_t count)
  -> arrow::Status {
  return match(ty, [&]<class Ty>(const Ty& ty) {
    return append_array_slice(as<type_to_arrow_builder_t<Ty>>(builder), ty,
                              as<type_to_arrow_array_t<Ty>>(array), begin,
                              count);
  });
}

// Make sure that `append_array_slice<...>` is emitted for every type.
template <std::monostate>
struct instantiate_append_array_slice {
  template <class... T>
  struct inner {
    static constexpr auto value = std::tuple{&append_array_slice<T>...};
  };

  static constexpr auto value
    = detail::tl_apply_t<concrete_types, inner>::value;
};

template struct instantiate_append_array_slice<std::monostate{}>;

} // namespace tenzir
