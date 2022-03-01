//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice.hpp"

#include "vast/arrow_extension_types.hpp"
#include "vast/arrow_table_slice_builder.hpp"
#include "vast/config.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
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

// -- utility class for mapping Arrow lists to VAST container views ------------

template <class Array>
data_view value_at(const type& t, const Array& arr, size_t row);

template <class T>
class arrow_container_view : public container_view<T> {
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

class arrow_record_view
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
struct decodable : std::false_type {};

template <>
struct decodable<bool_type, arrow::BooleanArray> : std::true_type {};

template <class TypeClass>
  requires(arrow::is_floating_type<TypeClass>::value)
struct decodable<real_type, arrow::NumericArray<TypeClass>> : std::true_type {};

template <class TypeClass>
  requires(std::is_integral_v<typename TypeClass::c_type>&&
             std::is_signed_v<typename TypeClass::c_type>)
struct decodable<integer_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(std::is_integral_v<typename TypeClass::c_type>&&
             std::is_signed_v<typename TypeClass::c_type>)
struct decodable<duration_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <class TypeClass>
  requires(
    std::is_integral_v<
      typename TypeClass::c_type> && !std::is_signed_v<typename TypeClass::c_type>)
struct decodable<count_type, arrow::NumericArray<TypeClass>> : std::true_type {
};

template <class TypeClass>
  requires(
    std::is_integral_v<
      typename TypeClass::c_type> && !std::is_signed_v<typename TypeClass::c_type>)
struct decodable<enumeration_type, arrow::NumericArray<TypeClass>>
  : std::true_type {};

template <>
struct decodable<address_type, arrow::FixedSizeBinaryArray> : std::true_type {};

template <>
struct decodable<subnet_type, arrow::FixedSizeBinaryArray> : std::true_type {};

template <>
struct decodable<string_type, arrow::StringArray> : std::true_type {};

template <>
struct decodable<pattern_type, arrow::StringArray> : std::true_type {};

template <>
struct decodable<time_type, arrow::TimestampArray> : std::true_type {};

template <>
struct decodable<list_type, arrow::ListArray> : std::true_type {};

template <>
struct decodable<map_type, arrow::ListArray> : std::true_type {};

template <>
struct decodable<record_type, arrow::StructArray> : std::true_type {};

template <class F>
auto decode(const type& t, const arrow::Array& arr, F& f) ->
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

auto boolean_at(const arrow::BooleanArray& arr, int64_t row) {
  return arr.Value(row);
}

auto real_at = [](const auto& arr, int64_t row) {
  return static_cast<real>(arr.Value(row));
};

auto integer_at = [](const auto& arr, int64_t row) {
  return integer{arr.Value(row)};
};

auto count_at = [](const auto& arr, int64_t row) {
  return static_cast<count>(arr.Value(row));
};

auto enumeration_at = [](const auto& arr, int64_t row) {
  return static_cast<enumeration>(arr.Value(row));
};

auto duration_at = [](const auto& arr, int64_t row) {
  return duration{arr.Value(row)};
};

auto string_at(const arrow::StringArray& arr, int64_t row) {
  auto offset = arr.value_offset(row);
  auto len = arr.value_length(row);
  auto buf = arr.value_data();
  auto cstr = reinterpret_cast<const char*>(buf->data() + offset);
  return std::string_view{cstr, detail::narrow_cast<size_t>(len)};
}

auto pattern_at(const arrow::StringArray& arr, int64_t row) {
  return pattern_view{string_at(arr, row)};
}

auto address_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 16);
  auto span = std::span<const uint8_t, 16>{bytes, 16};
  return address::v6(span);
}

auto subnet_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 17);
  auto span = std::span<const uint8_t, 16>{bytes, 16};
  return subnet{address::v6(span), bytes[16]};
}

auto timestamp_at(const arrow::TimestampArray& arr, int64_t row) {
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
}

auto container_view_at(type value_type, const arrow::ListArray& arr,
                       int64_t row) {
  auto offset = arr.value_offset(row);
  auto length = arr.value_length(row);
  using view_impl = arrow_container_view<data_view>;
  return caf::make_counted<view_impl>(std::move(value_type), arr.values(),
                                      offset, length);
}

auto list_at(type value_type, const arrow::ListArray& arr, int64_t row) {
  auto ptr = container_view_at(std::move(value_type), arr, row);
  return list_view_handle{list_view_ptr{std::move(ptr)}};
}

auto map_at(type key_type, type value_type, const arrow::ListArray& arr,
            int64_t row) {
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
}

auto record_at(const record_type& type, const arrow::StructArray& arr,
               int64_t row) {
  auto ptr = caf::make_counted<arrow_record_view>(type, arr, row);
  return record_view_handle{record_view_ptr{std::move(ptr)}};
}

class row_picker {
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
data_view value_at(const type& t, const Array& arr, size_t row) {
  row_picker f{row};
  decode(t, arr, f);
  return std::move(f.result());
}

// -- access to entire column --------------------------------------------------

class index_applier {
public:
  using result_type = void;

  index_applier(size_t offset, value_index& idx)
    : offset_(detail::narrow_cast<int64_t>(offset)), idx_(idx) {
    // nop
  }

  template <class Array, class Getter>
  void apply(const Array& arr, Getter f) {
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

} // namespace legacy

data_view value_at(const type& t, const arrow::Array& arr, int64_t row);

template <class T>
class experimental_container_view : public container_view<T> {
public:
  using super = container_view<T>;

  using size_type = typename super::size_type;

  using value_type = typename super::value_type;

  using array_ptr = std::shared_ptr<arrow::Array>;

  experimental_container_view(type element_type, array_ptr arr, int32_t offset,
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

class experimental_record_view
  : public container_view<std::pair<std::string_view, data_view>> {
public:
  explicit experimental_record_view(record_type type,
                                    const arrow::StructArray& arr, int64_t row)
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

// -- access to a single element -----------------------------------------------
auto enumeration_at = [](const arrow::DictionaryArray& arr, int64_t row) {
  const auto& b = static_cast<const arrow::Int16Array&>(*arr.indices());
  return static_cast<enumeration>(b.Value(row));
};

auto duration_at(const arrow::DurationArray& arr, int64_t row) {
  auto ts_value = arr.Value(row);
  const auto& ts_type = static_cast<const arrow::DurationType&>(*arr.type());
  switch (ts_type.unit()) {
    case arrow::TimeUnit::NANO: {
      return duration{ts_value};
    }
    case arrow::TimeUnit::MICRO: {
      auto x = std::chrono::microseconds{ts_value};
      return std::chrono::duration_cast<duration>(x);
    }
    case arrow::TimeUnit::MILLI: {
      auto x = std::chrono::milliseconds{ts_value};
      return std::chrono::duration_cast<duration>(x);
    }
    case arrow::TimeUnit::SECOND: {
      auto x = std::chrono::seconds{ts_value};
      return std::chrono::duration_cast<duration>(x);
    }
  }
  die("unhandled duration column time unit");
}

auto string_at(const arrow::StringArray& arr, int64_t row) {
  auto offset = arr.value_offset(row);
  auto len = arr.value_length(row);
  auto buf = arr.value_data();
  auto cstr = reinterpret_cast<const char*>(buf->data() + offset);
  return std::string_view{cstr, detail::narrow_cast<size_t>(len)};
}

auto address_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 16);
  auto span = std::span<const uint8_t, 16>{bytes, 16};
  return address::v6(span);
}

auto subnet_at(const arrow::StructArray& arr, int64_t row) {
  const auto& ext_arr
    = static_pointer_cast<arrow::ExtensionArray>(arr.field(0));
  const auto& address_array = *ext_arr->storage();
  const auto& length_array = arr.field(1);
  auto addr = address_at(
    static_cast<const arrow::FixedSizeBinaryArray&>(address_array), row);
  auto len = static_cast<const arrow::UInt8Array&>(*length_array).Value(row);
  return subnet{addr, uint8_t(len)};
}

auto timestamp_at(const arrow::TimestampArray& arr, int64_t row) {
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
}

auto container_view_at(type value_type, const arrow::ListArray& arr,
                       int64_t row) {
  auto offset = arr.value_offset(row);
  auto length = arr.value_length(row);
  using view_impl = experimental_container_view<data_view>;
  return caf::make_counted<view_impl>(std::move(value_type), arr.values(),
                                      offset, length);
}

auto list_at(type value_type, const arrow::ListArray& arr, int64_t row) {
  auto ptr = container_view_at(std::move(value_type), arr, row);
  return list_view_handle{list_view_ptr{std::move(ptr)}};
}

auto map_at(type key_type, type value_type, const arrow::MapArray& arr,
            int64_t row) {
  using view_impl
    = experimental_container_view<std::pair<data_view, data_view>>;
  auto offset = arr.value_offset(row);
  auto length = arr.value_length(row);
  auto kvp_type = type{record_type{
    {"key", key_type},
    {"value", value_type},
  }};
  auto ptr = caf::make_counted<view_impl>(std::move(kvp_type), arr.values(),
                                          offset, length);
  return map_view_handle{map_view_ptr{std::move(ptr)}};
}

auto record_at(const record_type& type, const arrow::StructArray& arr,
               int64_t row) {
  auto ptr = caf::make_counted<experimental_record_view>(type, arr, row);
  return record_view_handle{record_view_ptr{std::move(ptr)}};
}

data_view value_at(const type& t, const arrow::Array& arr, int64_t row) {
  auto f = detail::overload{
    [&](const bool_type&, const arrow::BooleanArray& a) {
      return a.Value(row);
    },
    [&](const integer_type&, const arrow::Int64Array& a) {
      return integer{a.Value(row)};
    },
    [&](const count_type&, const arrow::UInt64Array& a) {
      return a.Value(row);
    },
    [&](const real_type&, const arrow::DoubleArray& a) {
      return a.Value(row);
    },
    [&](const enumeration_type&, const enum_array& a) {
      return enumeration_at(
        static_cast<const arrow::DictionaryArray&>(*a.storage()), row);
    },
    [&](const time_type&, const arrow::TimestampArray& a) {
      return timestamp_at(a, row);
    },
    [&](const duration_type&, const arrow::DurationArray& a) {
      return duration_at(a, row);
    },
    [&](const address_type&, const address_array& a) {
      return address_at(
        static_cast<const arrow::FixedSizeBinaryArray&>(*a.storage()), row);
    },
    [&](const subnet_type&, const subnet_array& a) {
      return subnet_at(static_cast<const arrow::StructArray&>(*a.storage()),
                       row);
    },
    [&](const string_type&, const arrow::StringArray& a) {
      return string_at(a, row);
    },
    [&](const pattern_type&, const pattern_array& a) {
      return pattern_view{
        string_at(static_cast<const arrow::StringArray&>(*a.storage()), row)};
    },
    [&](const list_type& lt, const arrow::ListArray& a) {
      return list_at(lt.value_type(), a, row);
    },
    [&](const map_type& mt, const arrow::MapArray& a) {
      return map_at(mt.key_type(), mt.value_type(), a, row);
    },
    [&](const record_type& rt, const arrow::StructArray& a) {
      return record_at(rt, a, row);
    },
    [&](const auto&, const auto&) -> data_view {
      die(fmt::format("unhandled type pair({}, {})!\n", t,
                      arr.type()->ToString()));
    },
  };
  if (arr.IsNull(row))
    return caf::none;
  return caf::visit(f, t, arr);
}

template <concrete_type Type, class Array>
auto values([[maybe_unused]] const Type& type, const Array& arr)
  -> detail::generator<std::optional<view<type_to_data_t<Type>>>> {
  auto type_mismatch = [&]() {
    VAST_ASSERT(false, "type mismatch");
    __builtin_unreachable();
  };
  for (int i = 0; i < arr.length(); ++i) {
    if (arr.IsNull(i)) {
      co_yield {};
    } else {
      if constexpr (std::is_same_v<Type, bool_type>) {
        if constexpr (std::is_same_v<Array, arrow::BooleanArray>) {
          co_yield arr.Value(i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, integer_type>) {
        if constexpr (std::is_same_v<Array, arrow::Int64Array>) {
          co_yield integer{arr.Value(i)};
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, count_type>) {
        if constexpr (std::is_same_v<Array, arrow::UInt64Array>) {
          co_yield arr.Value(i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, real_type>) {
        if constexpr (std::is_same_v<Array, arrow::DoubleArray>) {
          co_yield arr.Value(i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, enumeration_type>) {
        if constexpr (std::is_same_v<Array, enum_array>) {
          co_yield enumeration_at(
            static_cast<const arrow::DictionaryArray&>(*arr.storage()), i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, time_type>) {
        if constexpr (std::is_same_v<Array, arrow::TimestampArray>) {
          co_yield timestamp_at(arr, i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, duration_type>) {
        if constexpr (std::is_same_v<Array, arrow::DurationArray>) {
          co_yield duration_at(arr, i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, address_type>) {
        if constexpr (std::is_same_v<Array, address_array>) {
          co_yield address_at(
            static_cast<const arrow::FixedSizeBinaryArray&>(*arr.storage()), i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, subnet_type>) {
        if constexpr (std::is_same_v<Array, subnet_array>) {
          co_yield subnet_at(
            static_cast<const arrow::StructArray&>(*arr.storage()), i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, string_type>) {
        if constexpr (std::is_same_v<Array, arrow::StringArray>) {
          co_yield string_at(arr, i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, pattern_type>) {
        if constexpr (std::is_same_v<Array, pattern_array>) {
          co_yield pattern_view{string_at(
            static_cast<const arrow::StringArray&>(*arr.storage()), i)};
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, list_type>) {
        if constexpr (std::is_same_v<Array, arrow::ListArray>) {
          co_yield list_at(type.value_type(), arr, i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, map_type>) {
        if constexpr (std::is_same_v<Array, arrow::MapArray>) {
          co_yield map_at(type.key_type(), type.value_type(), arr, i);
        } else {
          type_mismatch();
        }
      } else if constexpr (std::is_same_v<Type, record_type>) {
        if constexpr (std::is_same_v<Array, arrow::StructArray>) {
          // TODO: record_at is expensive, breaking the columnar processing
          co_yield record_at(type, arr, i);
        } else {
          type_mismatch();
        }
      } else {
        static_assert(detail::always_false_v<Type>, "unhandled vast type");
      }
    }
  }
}

auto values(const type& type, const arrow::Array& array)
  -> detail::generator<data_view> {
  auto f = [](const concrete_type auto& type,
              const auto& array) -> detail::generator<data_view> {
    for (auto&& result : values(type, array)) {
      if (result)
        co_yield *result;
      else
        co_yield caf::none;
    }
  };
  return caf::visit(f, type, array);
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

  std::shared_ptr<arrow::RecordBatch> legacy_decode(
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
    [&](const std::shared_ptr<arrow::Array>& a) {
      out.push_back(a);
    },
    [&](const std::shared_ptr<arrow::StructArray>& s) {
      for (const auto& child : s->fields())
        index_column_arrays(child, out);
    },
  };
  return caf::visit(f, arr);
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
    // This legacy type has to stay; it is deserialized from disk.
    auto intermediate = legacy_record_type{};
    if (auto err = fbs::deserialize_bytes(slice_.layout(), intermediate))
      die("failed to deserialize layout: " + render(err));
    state_.layout = type::from_legacy_type(intermediate);
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.legacy_decode(
      as_arrow_buffer(parent->slice(as_bytes(*slice.schema()))),
      as_arrow_buffer(parent->slice(as_bytes(*slice.record_batch()))));
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v1>) {
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
    state_.layout = make_vast_type(*state_.record_batch->schema());
    VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
    state_.flat_columns = index_column_arrays(state_.record_batch);
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
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
    if (auto&& batch = record_batch()) {
      auto f = legacy::index_applier{offset, index};
      auto array = batch->column(detail::narrow_cast<int>(column));
      const auto& layout = caf::get<record_type>(this->layout());
      auto o = layout.resolve_flat_index(column);
      legacy::decode(layout.field(o).type, *array, f);
    }
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
    auto&& batch = record_batch();
    VAST_ASSERT(batch);
    auto array = batch->column(detail::narrow_cast<int>(column));
    const auto& layout = caf::get<record_type>(this->layout());
    auto offset = layout.resolve_flat_index(column);
    return legacy::value_at(layout.field(offset).type, *array, row);
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
    VAST_ASSERT(congruent(
      caf::get<record_type>(this->layout())
        .field(caf::get<record_type>(this->layout()).resolve_flat_index(column))
        .type,
      t));
    auto&& batch = record_batch();
    VAST_ASSERT(batch);
    auto array = batch->column(detail::narrow_cast<int>(column));
    return legacy::value_at(t, *array, row);
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
