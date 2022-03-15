//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/passthrough.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/experimental_table_slice_builder.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <type_traits>
#include <utility>

namespace vast {

namespace {

namespace legacy {

VAST_DIAGNOSTIC_PUSH
VAST_DIAGNOSTIC_IGNORE_DEPRECATED

// -- utility class for mapping Arrow lists to VAST container views ------------

template <class Array>
[[deprecated]] data_view value_at(const type& t, const Array& arr, size_t row);

template <class T>
class [[deprecated]] arrow_container_view : public container_view<T> {
public:
  using super = container_view<T>;

  using size_type = typename super::size_type;

  using value_type = typename super::value_type;

  using array_ptr = std::shared_ptr<arrow::Array>;

  arrow_container_view(type element_type, array_ptr arr, int32_t offset,
                       int32_t length)
    : element_type_(std::move(element_type)),
      offset_(offset),
      length_(length),
      arr_{std::move(arr)} {
    // nop
  }

  value_type at(size_type row) const override {
    auto adjusted_row = row + detail::narrow_cast<size_type>(offset_);
    if constexpr (std::is_same_v<value_type, data_view>) {
      return value_at(element_type_, *arr_, adjusted_row);
    } else {
      using expected_type = std::pair<data_view, data_view>;
      static_assert(std::is_same_v<value_type, expected_type>);
      if (const auto* dt = caf::get_if<record_type>(&element_type_)) {
        if (dt->num_fields() == 2) {
          const auto& arr = static_cast<const arrow::StructArray&>(*arr_);
          auto key_arr = arr.field(0);
          auto value_arr = arr.field(1);
          return {
            value_at(dt->field(0).type, *key_arr, adjusted_row),
            value_at(dt->field(1).type, *value_arr, adjusted_row),
          };
        }
      }
      return {
        caf::none,
        caf::none,
      };
    }
  }

  size_type size() const noexcept override {
    return detail::narrow_cast<size_t>(length_);
  }

private:
  type element_type_;
  int32_t offset_;
  int32_t length_;
  std::shared_ptr<arrow::Array> arr_;
};

class [[deprecated]] arrow_record_view
  : public container_view<std::pair<std::string_view, data_view>> {
public:
  explicit arrow_record_view(record_type type, const arrow::StructArray& arr,
                             int64_t row)
    : type_{std::move(type)}, arr_{arr}, row_{row} {
    // nop
  }

  value_type at(size_type i) const override {
    const auto& field = type_.field(i);
    auto col = arr_.field(i);
    VAST_ASSERT(col);
    VAST_ASSERT(col->Equals(arr_.GetFieldByName(std::string{field.name})));
    return {field.name, value_at(field.type, *col, row_)};
  }

  size_type size() const noexcept override {
    return arr_.num_fields();
  }

private:
  const record_type type_;
  const arrow::StructArray& arr_;
  const int64_t row_;
};

// -- decoding of Arrow column arrays ------------------------------------------

// Safe ourselves redundant boilerplate code for dispatching to the visitor.
template <concrete_type Type, class Array>
struct [[deprecated]] decodable : std::false_type {};

template <>
struct [[deprecated]] decodable<bool_type, arrow::BooleanArray>
  : std::true_type {};

template <class TypeClass>
  requires(arrow::is_floating_type<TypeClass>::value)
struct [[deprecated]] decodable<real_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(std::is_integral_v<typename TypeClass::c_type>&&
             std::is_signed_v<typename TypeClass::c_type>)
struct [[deprecated]] decodable<integer_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(std::is_integral_v<typename TypeClass::c_type>&&
             std::is_signed_v<typename TypeClass::c_type>)
struct [[deprecated]] decodable<duration_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(
    std::is_integral_v<
      typename TypeClass::c_type> && !std::is_signed_v<typename TypeClass::c_type>)
struct [[deprecated]] decodable<count_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(
    std::is_integral_v<
      typename TypeClass::c_type> && !std::is_signed_v<typename TypeClass::c_type>)
struct [[deprecated]] decodable<enumeration_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<address_type, arrow::FixedSizeBinaryArray>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<subnet_type, arrow::FixedSizeBinaryArray>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<string_type, arrow::StringArray>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<pattern_type, arrow::StringArray>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<time_type, arrow::TimestampArray>
  : std::true_type {};

template <>
struct [[deprecated]] decodable<list_type, arrow::ListArray> : std::true_type {
};

template <>
struct [[deprecated]] decodable<map_type, arrow::ListArray> : std::true_type {};

template <>
struct [[deprecated]] decodable<record_type, arrow::StructArray>
  : std::true_type {};

template <class F>
[[deprecated]] auto decode(const type& t, const arrow::Array& arr, F& f) ->
  typename F::result_type {
  auto dispatch = [&]<class Array>(const Array& arr) {
    auto visitor
      = [&]<concrete_type Type>(const Type& t) -> typename F::result_type {
      if constexpr (decodable<Type, Array>::value)
        return f(arr, t);
      else if constexpr (std::is_void_v<typename F::result_type>)
        VAST_ERROR("unable to decode {} into {}", detail::pretty_type_name(arr),
                   t);
      else
        die(fmt::format("unable to decode {} into {}",
                        detail::pretty_type_name(arr), t));
    };
    return caf::visit(visitor, t);
  };
  switch (arr.type_id()) {
    default: {
      VAST_WARN("{} got an unrecognized Arrow type ID", __func__);
      break;
    }
    // -- handle basic types ---------------------------------------------------
    case arrow::Type::BOOL: {
      return dispatch(static_cast<const arrow::BooleanArray&>(arr));
    }
    case arrow::Type::STRING: {
      return dispatch(static_cast<const arrow::StringArray&>(arr));
    }
    case arrow::Type::TIMESTAMP: {
      return dispatch(static_cast<const arrow::TimestampArray&>(arr));
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      using array_type = arrow::FixedSizeBinaryArray;
      return dispatch(static_cast<const array_type&>(arr));
    }
    // -- handle container types -----------------------------------------------
    case arrow::Type::LIST: {
      return dispatch(static_cast<const arrow::ListArray&>(arr));
    }
    case arrow::Type::STRUCT: {
      return dispatch(static_cast<const arrow::StructArray&>(arr));
    }
    // -- lift floating point values to real -----------------------------
    case arrow::Type::HALF_FLOAT: {
      using array_type = arrow::NumericArray<arrow::HalfFloatType>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::FLOAT: {
      using array_type = arrow::NumericArray<arrow::FloatType>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::DOUBLE: {
      using array_type = arrow::NumericArray<arrow::DoubleType>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    // -- lift singed values to integer ----------------------------------
    case arrow::Type::INT8: {
      using array_type = arrow::NumericArray<arrow::Int8Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::INT16: {
      using array_type = arrow::NumericArray<arrow::Int16Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::INT32: {
      using array_type = arrow::NumericArray<arrow::Int32Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::INT64: {
      using array_type = arrow::NumericArray<arrow::Int64Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    // -- lift unsinged values to count ----------------------------------
    case arrow::Type::UINT8: {
      using array_type = arrow::NumericArray<arrow::UInt8Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::UINT16: {
      using array_type = arrow::NumericArray<arrow::UInt16Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::UINT32: {
      using array_type = arrow::NumericArray<arrow::UInt32Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
    case arrow::Type::UINT64: {
      using array_type = arrow::NumericArray<arrow::UInt64Type>;
      return dispatch(static_cast<const array_type&>(arr));
    }
  }
}

// -- access to a single element -----------------------------------------------

[[deprecated]] auto boolean_at
  = [](const arrow::BooleanArray& arr, int64_t row) {
      return arr.Value(row);
    };

[[deprecated]] auto real_at = [](const auto& arr, int64_t row) {
  return static_cast<real>(arr.Value(row));
};

[[deprecated]] auto integer_at = [](const auto& arr, int64_t row) {
  return integer{arr.Value(row)};
};

[[deprecated]] auto count_at = [](const auto& arr, int64_t row) {
  return static_cast<count>(arr.Value(row));
};

[[deprecated]] auto enumeration_at = [](const auto& arr, int64_t row) {
  return static_cast<enumeration>(arr.Value(row));
};

[[deprecated]] auto duration_at = [](const auto& arr, int64_t row) {
  return duration{arr.Value(row)};
};

[[deprecated]] auto string_at = [](const arrow::StringArray& arr, int64_t row) {
  auto offset = arr.value_offset(row);
  auto len = arr.value_length(row);
  auto buf = arr.value_data();
  auto cstr = reinterpret_cast<const char*>(buf->data() + offset);
  return std::string_view{cstr, detail::narrow_cast<size_t>(len)};
};

[[deprecated]] auto pattern_at
  = [](const arrow::StringArray& arr, int64_t row) {
      return pattern_view{string_at(arr, row)};
    };

[[deprecated]] auto address_at
  = [](const arrow::FixedSizeBinaryArray& arr, int64_t row) {
      auto bytes = arr.raw_values() + (row * 16);
      auto span = std::span<const uint8_t, 16>{bytes, 16};
      return address::v6(span);
    };

[[deprecated]] auto subnet_at
  = [](const arrow::FixedSizeBinaryArray& arr, int64_t row) {
      auto bytes = arr.raw_values() + (row * 17);
      auto span = std::span<const uint8_t, 16>{bytes, 16};
      return subnet{address::v6(span), bytes[16]};
    };

[[deprecated]] auto timestamp_at
  = [](const arrow::TimestampArray& arr, int64_t row) {
      auto ts_value = static_cast<integer::value_type>(arr.Value(row));
      duration time_since_epoch{0};
      auto& ts_type = static_cast<const arrow::TimestampType&>(*arr.type());
      switch (ts_type.unit()) {
        case arrow::TimeUnit::NANO: {
          time_since_epoch = duration{ts_value};
          break;
        }
        case arrow::TimeUnit::MICRO: {
          auto x = std::chrono::microseconds{ts_value};
          time_since_epoch = std::chrono::duration_cast<duration>(x);
          break;
        }
        case arrow::TimeUnit::MILLI: {
          auto x = std::chrono::milliseconds{ts_value};
          time_since_epoch = std::chrono::duration_cast<duration>(x);
          break;
        }
        case arrow::TimeUnit::SECOND: {
          auto x = std::chrono::seconds{ts_value};
          time_since_epoch = std::chrono::duration_cast<duration>(x);
          break;
        }
      }
      return time{time_since_epoch};
    };

[[deprecated]] auto container_view_at
  = [](type value_type, const arrow::ListArray& arr, int64_t row) {
      auto offset = arr.value_offset(row);
      auto length = arr.value_length(row);
      using view_impl = arrow_container_view<data_view>;
      return caf::make_counted<view_impl>(std::move(value_type), arr.values(),
                                          offset, length);
    };

[[deprecated]] auto list_at
  = [](type value_type, const arrow::ListArray& arr, int64_t row) {
      auto ptr = container_view_at(std::move(value_type), arr, row);
      return list_view_handle{list_view_ptr{std::move(ptr)}};
    };

[[deprecated]] auto map_at = [](type key_type, type value_type,
                                const arrow::ListArray& arr, int64_t row) {
  using view_impl = arrow_container_view<std::pair<data_view, data_view>>;
  auto offset = arr.value_offset(row);
  auto length = arr.value_length(row);
  auto kvp_type = type{record_type{
    {"key", key_type},
    {"value", value_type},
  }};
  auto ptr = caf::make_counted<view_impl>(std::move(kvp_type), arr.values(),
                                          offset, length);
  return map_view_handle{map_view_ptr{std::move(ptr)}};
};

[[deprecated]] auto record_at
  = [](const record_type& type, const arrow::StructArray& arr, int64_t row) {
      auto ptr = caf::make_counted<arrow_record_view>(type, arr, row);
      return record_view_handle{record_view_ptr{std::move(ptr)}};
    };

class [[deprecated]] row_picker {
public:
  using result_type = void;

  row_picker(size_t row) : row_(detail::narrow_cast<int64_t>(row)) {
    // nop
  }

  data_view& result() {
    return result_;
  }

  void operator()(const arrow::BooleanArray& arr, const bool_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = boolean_at(arr, row_);
  }

  template <class T, class U>
  void operator()(const arrow::NumericArray<T>& arr, const U&) {
    if (arr.IsNull(row_))
      return;
    if constexpr (detail::is_any_v<U, real_type, integer_type, count_type,
                                   enumeration_type>) {
      using view_type = view<type_to_data_t<U>>;
      result_ = static_cast<view_type>(arr.Value(row_));
    } else {
      static_assert(std::is_same_v<U, duration_type>);
      result_ = duration_at(arr, row_);
    }
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const integer_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = integer_at(arr, row_);
  }

  void operator()(const arrow::FixedSizeBinaryArray& arr, const address_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = address_at(arr, row_);
  }

  void operator()(const arrow::FixedSizeBinaryArray& arr, const subnet_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = subnet_at(arr, row_);
  }

  template <class T>
  void operator()(const arrow::StringArray& arr, const T&) {
    if (arr.IsNull(row_))
      return;
    if constexpr (std::is_same_v<T, string_type>) {
      result_ = string_at(arr, row_);
    } else {
      static_assert(std::is_same_v<T, pattern_type>);
      result_ = pattern_at(arr, row_);
    }
  }

  void operator()(const arrow::TimestampArray& arr, const time_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = timestamp_at(arr, row_);
  }

  template <class T>
  void operator()(const arrow::ListArray& arr, const T& t) {
    if (arr.IsNull(row_))
      return;
    if constexpr (std::is_same_v<T, list_type>) {
      result_ = list_at(t.value_type(), arr, row_);
    } else {
      static_assert(std::is_same_v<T, map_type>);
      result_ = map_at(t.key_type(), t.value_type(), arr, row_);
    }
  }

  void operator()(const arrow::StructArray& arr, const record_type& t) {
    if (arr.IsNull(row_))
      return;
    result_ = record_at(t, arr, row_);
  }

private:
  data_view result_;
  int64_t row_;
};

template <class Array>
[[deprecated]] data_view value_at(const type& t, const Array& arr, size_t row) {
  row_picker f{row};
  decode(t, arr, f);
  return std::move(f.result());
}

// -- access to entire column --------------------------------------------------

class [[deprecated]] index_applier {
public:
  using result_type = void;

  index_applier(size_t offset, value_index& idx)
    : offset_(detail::narrow_cast<int64_t>(offset)), idx_(idx) {
    // nop
  }

  template <class Array, class Getter>
  void apply(const Array& arr, Getter&& f) {
    for (int64_t row = 0; row < arr.length(); ++row)
      if (!arr.IsNull(row))
        idx_.append(f(arr, row), detail::narrow_cast<size_t>(offset_ + row));
  }

  void operator()(const arrow::BooleanArray& arr, const bool_type&) {
    apply(arr, boolean_at);
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const real_type&) {
    apply(arr, real_at);
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const integer_type&) {
    apply(arr, integer_at);
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const count_type&) {
    apply(arr, count_at);
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const enumeration_type&) {
    apply(arr, enumeration_at);
  }

  template <class T>
  void operator()(const arrow::NumericArray<T>& arr, const duration_type&) {
    apply(arr, duration_at);
  }

  void operator()(const arrow::FixedSizeBinaryArray& arr, const address_type&) {
    apply(arr, address_at);
  }

  void operator()(const arrow::FixedSizeBinaryArray& arr, const subnet_type&) {
    apply(arr, subnet_at);
  }

  void operator()(const arrow::StringArray& arr, const string_type&) {
    apply(arr, string_at);
  }

  void operator()(const arrow::StringArray& arr, const pattern_type&) {
    apply(arr, pattern_at);
  }

  void operator()(const arrow::TimestampArray& arr, const time_type&) {
    apply(arr, timestamp_at);
  }

  template <class T>
  void operator()(const arrow::ListArray& arr, const T& t) {
    if constexpr (std::is_same_v<T, list_type>) {
      auto f = [&](const auto& arr, int64_t row) {
        return list_at(t.value_type(), arr, row);
      };
      apply(arr, f);
    } else {
      static_assert(std::is_same_v<T, map_type>);
      auto f = [&](const auto& arr, int64_t row) {
        return map_at(t.key_type(), t.value_type(), arr, row);
      };
      apply(arr, f);
    }
  }

  void operator()(const arrow::StructArray& arr, const record_type& t) {
    apply(arr, [&](const auto& arr, int64_t row) {
      return record_at(t, arr, row);
    });
  }

private:
  int64_t offset_;
  value_index& idx_;
};

VAST_DIAGNOSTIC_POP

} // namespace legacy

data_view value_at(const type& type, const std::same_as<arrow::Array> auto& arr,
                   int64_t row) noexcept;

template <concrete_type Type>
view<type_to_data_t<Type>>
value_at([[maybe_unused]] const Type& type,
         const type_to_arrow_array_t<Type>& arr, int64_t row) noexcept {
  VAST_ASSERT(!arr.IsNull(row));
  if constexpr (detail::is_any_v<Type, bool_type, count_type, real_type>) {
    return arr.GetView(row);
  } else if constexpr (std::is_same_v<Type, integer_type>) {
    return integer{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, duration_type>) {
    VAST_ASSERT(
      caf::get<type_to_arrow_type_t<duration_type>>(*arr.type()).unit()
      == arrow::TimeUnit::NANO);
    return duration{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, time_type>) {
    VAST_ASSERT(caf::get<type_to_arrow_type_t<time_type>>(*arr.type()).unit()
                == arrow::TimeUnit::NANO);
    return time{} + duration{arr.GetView(row)};
  } else if constexpr (std::is_same_v<Type, string_type>) {
    const auto str = arr.GetView(row);
    return {str.data(), str.size()};
  } else if constexpr (std::is_same_v<Type, pattern_type>) {
    const auto& storage
      = caf::get<type_to_arrow_array_t<string_type>>(*arr.storage());
    return view<type_to_data_t<pattern_type>>{
      value_at(string_type{}, storage, row)};
  } else if constexpr (std::is_same_v<Type, address_type>) {
    const auto& storage
      = static_cast<const arrow::FixedSizeBinaryArray&>(*arr.storage());
    VAST_ASSERT(storage.byte_width() == 16);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const auto* bytes = storage.raw_values() + (row * 16);
    return address::v6(std::span<const uint8_t, 16>{bytes, 16});
  } else if constexpr (std::is_same_v<Type, subnet_type>) {
    const auto& storage
      = static_cast<const arrow::StructArray&>(*arr.storage());
    VAST_ASSERT(storage.num_fields() == 2);
    auto network = value_at(
      address_type{},
      caf::get<type_to_arrow_array_t<address_type>>(*storage.field(0)), row);
    auto length
      = static_cast<const arrow::UInt8Array&>(*storage.field(1)).GetView(row);
    return {network, length};
  } else if constexpr (std::is_same_v<Type, enumeration_type>) {
    const auto& storage
      = static_cast<const arrow::DictionaryArray&>(*arr.storage());
    return detail::narrow_cast<view<type_to_data_t<enumeration_type>>>(
      storage.GetValueIndex(row));
  } else if constexpr (std::is_same_v<Type, list_type>) {
    auto f = [&]<concrete_type ValueType>(
               const ValueType& value_type) -> list_view_handle {
      struct list_view final : list_view_handle::view_type {
        list_view(ValueType value_type,
                  std::shared_ptr<arrow::Array> value_slice) noexcept
          : value_type{std::move(value_type)},
            value_slice{std::move(value_slice)} {
          // nop
        }
        value_type at(size_type i) const override {
          const auto row = detail::narrow_cast<int64_t>(i);
          if (value_slice->IsNull(row))
            return caf::none;
          return value_at(
            value_type,
            caf::get<type_to_arrow_array_t<ValueType>>(*value_slice), row);
        };
        size_type size() const noexcept override {
          return value_slice->length();
        };
        ValueType value_type;
        std::shared_ptr<arrow::Array> value_slice;
      };
      return list_view_handle{list_view_ptr{
        caf::make_counted<list_view>(value_type, arr.value_slice(row))}};
    };
    return caf::visit(f, type.value_type());
  } else if constexpr (std::is_same_v<Type, map_type>) {
    auto f = [&]<concrete_type KeyType, concrete_type ItemType>(
               const KeyType& key_type,
               const ItemType& item_type) -> map_view_handle {
      struct map_view final : map_view_handle::view_type {
        map_view(KeyType key_type, ItemType item_type,
                 std::shared_ptr<arrow::Array> key_array,
                 std::shared_ptr<arrow::Array> item_array, int64_t value_offset,
                 int64_t value_length)
          : key_type{std::move(key_type)},
            item_type{std::move(item_type)},
            key_array{std::move(key_array)},
            item_array{std::move(item_array)},
            value_offset{value_offset},
            value_length{value_length} {
          // nop
        }
        value_type at(size_type i) const override {
          VAST_ASSERT(!key_array->IsNull(value_offset + i));
          if (item_array->IsNull(value_offset + i))
            return {};
          return {
            value_at(key_type,
                     caf::get<type_to_arrow_array_t<KeyType>>(*key_array),
                     value_offset + i),
            value_at(item_type,
                     caf::get<type_to_arrow_array_t<ItemType>>(*item_array),
                     value_offset + i),
          };
        };
        size_type size() const noexcept override {
          return detail::narrow_cast<size_type>(value_length);
        };
        KeyType key_type;
        ItemType item_type;
        std::shared_ptr<arrow::Array> key_array;
        std::shared_ptr<arrow::Array> item_array;
        int64_t value_offset;
        int64_t value_length;
      };
      // Note that there's no `value_slice(...)` and `item_slice(...)` functions
      // for map arrays in Arrow similar to the `value_slice(...)` function for
      // list arrays, so we need to manually work with offsets and lengths here.
      return map_view_handle{map_view_ptr{caf::make_counted<map_view>(
        key_type, item_type, arr.keys(), arr.items(), arr.value_offset(row),
        arr.value_length(row))}};
    };
    return caf::visit(f, type.key_type(), type.value_type());
  } else if constexpr (std::is_same_v<Type, record_type>) {
    struct record_view final : record_view_handle::view_type {
      record_view(record_type type, arrow::ArrayVector fields, int64_t row)
        : type{std::move(type)}, fields{std::move(fields)}, row{row} {
        // nop
      }
      value_type at(size_type i) const override {
        const auto& field = type.field(i);
        return {
          field.name,
          value_at(field.type, *fields[i], row),
        };
      };
      size_type size() const noexcept override {
        return type.num_fields();
      };
      record_type type;
      arrow::ArrayVector fields;
      int64_t row;
    };
    return record_view_handle{
      record_view_ptr{caf::make_counted<record_view>(type, arr.fields(), row)}};
  } else {
    static_assert(detail::always_false_v<Type>, "unhandled type");
  }
}

data_view value_at(const type& type, const std::same_as<arrow::Array> auto& arr,
                   int64_t row) noexcept {
  if (arr.IsNull(row))
    return caf::none;
  auto f = [&]<concrete_type Type>(const Type& type) noexcept -> data_view {
    return value_at(type, caf::get<type_to_arrow_array_t<Type>>(arr), row);
  };
  return caf::visit(f, type);
}

template <concrete_type Type>
auto values(const Type& type, const type_to_arrow_array_t<Type>& arr)
  -> detail::generator<std::optional<view<type_to_data_t<Type>>>> {
  for (int i = 0; i < arr.length(); ++i) {
    if (arr.IsNull(i))
      co_yield {};
    else
      co_yield value_at(type, arr, i);
  }
}

auto values(const type& type, const std::same_as<arrow::Array> auto& array)
  -> detail::generator<data_view> {
  const auto f = []<concrete_type Type>(
                   const Type& type,
                   const arrow::Array& array) -> detail::generator<data_view> {
    for (auto&& result :
         values(type, caf::get<type_to_arrow_array_t<Type>>(array))) {
      if (!result)
        co_yield {};
      else
        co_yield std::move(*result);
    }
  };
  return caf::visit(f, type, detail::passthrough(array));
}

// -- utility for converting Buffer to RecordBatch -----------------------------

template <class Callback>
class record_batch_listener final : public arrow::ipc::Listener {
public:
  record_batch_listener(Callback&& callback) noexcept
    : callback_{std::forward<Callback>(callback)} {
    // nop
  }

  ~record_batch_listener() noexcept override = default;

private:
  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    std::invoke(callback_, std::move(record_batch));
    return arrow::Status::OK();
  }

  Callback callback_;
};

template <class Callback>
auto make_record_batch_listener(Callback&& callback) {
  return std::make_shared<record_batch_listener<Callback>>(
    std::forward<Callback>(callback));
}

class record_batch_decoder final {
public:
  record_batch_decoder() noexcept
    : decoder_{make_record_batch_listener(
      [&](std::shared_ptr<arrow::RecordBatch> record_batch) {
        record_batch_ = std::move(record_batch);
      })} {
    // nop
  }

  [[deprecated]] std::shared_ptr<arrow::RecordBatch> legacy_decode(
    const std::shared_ptr<arrow::Buffer>& flat_schema,
    const std::shared_ptr<arrow::Buffer>& flat_record_batch) noexcept {
    VAST_ASSERT(!record_batch_);
    if (auto status = decoder_.Consume(flat_schema); !status.ok()) {
      VAST_ERROR("{} failed to decode Arrow Schema: {}", __func__,
                 status.ToString());
      return {};
    }
    if (auto status = decoder_.Consume(flat_record_batch); !status.ok()) {
      VAST_ERROR("{} failed to decode Arrow Record Batch: {}", __func__,
                 status.ToString());
      return {};
    }
    VAST_ASSERT(record_batch_);
    return std::exchange(record_batch_, {});
  }

  std::shared_ptr<arrow::RecordBatch>
  decode(const std::shared_ptr<arrow::Buffer>& flat_record_batch) noexcept {
    VAST_ASSERT(!record_batch_);
    if (auto status = decoder_.Consume(flat_record_batch); !status.ok()) {
      VAST_ERROR("{} failed to decode Arrow Record Batch: {}", __func__,
                 status.ToString());
      return {};
    }
    VAST_ASSERT(record_batch_);
    return std::exchange(record_batch_, {});
  }

private:
  arrow::ipc::StreamDecoder decoder_;
  std::shared_ptr<arrow::RecordBatch> record_batch_ = nullptr;
};

/// Compute position for each array by traversing the schema tree breadth-first.
void index_column_arrays(const std::shared_ptr<arrow::Array>& arr,
                         arrow::ArrayVector& out) {
  auto f = detail::overload{
    [&](const auto&) {
      out.push_back(arr);
    },
    [&](const arrow::StructArray& s) {
      for (const auto& child : s.fields())
        index_column_arrays(child, out);
    },
  };
  return caf::visit(f, *arr);
}

arrow::ArrayVector
index_column_arrays(const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  arrow::ArrayVector result{};
  for (const auto& arr : record_batch->columns())
    index_column_arrays(arr, result);
  return result;
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::arrow_table_slice(
  const FlatBuffer& slice, [[maybe_unused]] const chunk_ptr& parent) noexcept
  : slice_{slice}, state_{} {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v0>) {
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    // This legacy type has to stay; it is deserialized from disk.
    auto intermediate = legacy_record_type{};
    if (auto err = fbs::deserialize_bytes(slice_.layout(), intermediate))
      die("failed to deserialize layout: " + render(err));
    state_.layout = type::from_legacy_type(intermediate);
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.legacy_decode(
      as_arrow_buffer(parent->slice(as_bytes(*slice.schema()))),
      as_arrow_buffer(parent->slice(as_bytes(*slice.record_batch()))));
    VAST_DIAGNOSTIC_POP
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v1>) {
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    // We decouple the sliced type from the layout intentionally. This is an
    // absolute must because we store the state in the deletion step of the
    // table slice's chunk, and storing a sliced chunk in there would cause a
    // cyclic reference. In the future, we should just not store the sliced
    // chunk at all, but rather create it on the fly only.
    state_.layout = type{chunk::copy(as_bytes(*slice_.layout()))};
    VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.legacy_decode(
      as_arrow_buffer(parent->slice(as_bytes(*slice.schema()))),
      as_arrow_buffer(parent->slice(as_bytes(*slice.record_batch()))));
    VAST_DIAGNOSTIC_POP
  } else if constexpr (std::is_same_v<FlatBuffer,
                                      fbs::table_slice::arrow::experimental>) {
    // We decouple the sliced type from the layout intentionally. This is an
    // absolute must because we store the state in the deletion step of the
    // table slice's chunk, and storing a sliced chunk in there would cause a
    // cyclic reference. In the future, we should just not store the sliced
    // chunk at all, but rather create it on the fly only.
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.decode(
      as_arrow_buffer(parent->slice(as_bytes(*slice.arrow_ipc()))));
    state_.layout = type::from_arrow(*state_.record_batch->schema());
    VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
    state_.flat_columns = index_column_arrays(state_.record_batch);
    VAST_ASSERT(state_.flat_columns.size()
                == caf::get<record_type>(state_.layout).num_leaves());
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
#if VAST_ENABLE_ASSERTIONS
  auto validate_status = state_.record_batch->Validate();
  VAST_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
#endif // VAST_ENABLE_ASSERTIONS
}

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::~arrow_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

template <class FlatBuffer>
const type& arrow_table_slice<FlatBuffer>::layout() const noexcept {
  return state_.layout;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::rows() const noexcept {
  if (auto&& batch = record_batch())
    return batch->num_rows();
  return 0;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::columns() const noexcept {
  if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v0,
                                 fbs::table_slice::arrow::v1>) {
    if (auto&& batch = record_batch())
      return batch->num_columns();
  } else if constexpr (std::is_same_v<FlatBuffer,
                                      fbs::table_slice::arrow::experimental>) {
    if (auto&& batch = record_batch())
      return state_.flat_columns.size();
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
  return 0;
}

// -- data access ------------------------------------------------------------

template <class FlatBuffer>
void arrow_table_slice<FlatBuffer>::append_column_to_index(
  id offset, table_slice::size_type column, value_index& index) const {
  if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v0,
                                 fbs::table_slice::arrow::v1>) {
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    if (auto&& batch = record_batch()) {
      auto f = legacy::index_applier{offset, index};
      auto array = batch->column(detail::narrow_cast<int>(column));
      const auto& layout = caf::get<record_type>(this->layout());
      auto o = layout.resolve_flat_index(column);
      legacy::decode(layout.field(o).type, *array, f);
    }
    VAST_DIAGNOSTIC_POP
  } else if constexpr (std::is_same_v<FlatBuffer,
                                      fbs::table_slice::arrow::experimental>) {
    if (auto&& batch = record_batch()) {
      auto&& array = state_.flat_columns[column];
      const auto& layout = caf::get<record_type>(this->layout());
      auto type = layout.field(layout.resolve_flat_index(column)).type;
      for (size_t row = 0; auto&& value : values(type, *array)) {
        if (!caf::holds_alternative<view<caf::none_t>>(value))
          index.append(value, offset + row);
        ++row;
      }
    }
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
data_view
arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                  table_slice::size_type column) const {
  if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v0,
                                 fbs::table_slice::arrow::v1>) {
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    auto&& batch = record_batch();
    VAST_ASSERT(batch);
    auto array = batch->column(detail::narrow_cast<int>(column));
    const auto& layout = caf::get<record_type>(this->layout());
    auto offset = layout.resolve_flat_index(column);
    return legacy::value_at(layout.field(offset).type, *array, row);
    VAST_DIAGNOSTIC_POP
  } else if constexpr (std::is_same_v<FlatBuffer,
                                      fbs::table_slice::arrow::experimental>) {
    auto&& array = state_.flat_columns[column];
    const auto& layout = caf::get<record_type>(this->layout());
    auto offset = layout.resolve_flat_index(column);
    return value_at(layout.field(offset).type, *array, row);
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
data_view arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                            table_slice::size_type column,
                                            const type& t) const {
  if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v0,
                                 fbs::table_slice::arrow::v1>) {
    VAST_DIAGNOSTIC_PUSH
    VAST_DIAGNOSTIC_IGNORE_DEPRECATED
    VAST_ASSERT(congruent(
      caf::get<record_type>(this->layout())
        .field(caf::get<record_type>(this->layout()).resolve_flat_index(column))
        .type,
      t));
    auto&& batch = record_batch();
    VAST_ASSERT(batch);
    auto array = batch->column(detail::narrow_cast<int>(column));
    return legacy::value_at(t, *array, row);
    VAST_DIAGNOSTIC_POP
  } else if constexpr (std::is_same_v<FlatBuffer,
                                      fbs::table_slice::arrow::experimental>) {
    VAST_ASSERT(congruent(
      caf::get<record_type>(this->layout())
        .field(caf::get<record_type>(this->layout()).resolve_flat_index(column))
        .type,
      t));
    auto&& array = state_.flat_columns[column];
    return value_at(t, *array, row);
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
}

template <class FlatBuffer>
time arrow_table_slice<FlatBuffer>::import_time() const noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v0>) {
    return {};
  } else if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v1,
                                        fbs::table_slice::arrow::experimental>) {
    return time{} + duration{slice_.import_time()};
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled table slice "
                                                      "encoding");
  }
}

template <class FlatBuffer>
void arrow_table_slice<FlatBuffer>::import_time(
  [[maybe_unused]] time import_time) noexcept {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v0>) {
    die("cannot set import time in arrow.v0 table slice encoding");
  } else if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v1,
                                        fbs::table_slice::arrow::experimental>) {
    auto result = const_cast<FlatBuffer&>(slice_).mutate_import_time(
      import_time.time_since_epoch().count());
    VAST_ASSERT(result, "failed to mutate import time");
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled table slice "
                                                      "encoding");
  }
}

template <class FlatBuffer>
std::shared_ptr<arrow::RecordBatch>
arrow_table_slice<FlatBuffer>::record_batch() const noexcept {
  return state_.record_batch;
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all Arrow encoding versions.
template class arrow_table_slice<fbs::table_slice::arrow::v0>;
template class arrow_table_slice<fbs::table_slice::arrow::v1>;
template class arrow_table_slice<fbs::table_slice::arrow::experimental>;

} // namespace vast
