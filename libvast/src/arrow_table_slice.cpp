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
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/status.h>

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
    return {field.name, ::vast::legacy::value_at(field.type, *col, row_)};
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

template <concrete_type Type>
auto values(const Type& type, const type_to_arrow_array_t<Type>& arr) noexcept
  -> detail::generator<std::optional<view<type_to_data_t<Type>>>> {
  auto impl = [](const Type& type,
                 const type_to_arrow_array_storage_t<Type>& arr) noexcept
    -> detail::generator<std::optional<view<type_to_data_t<Type>>>> {
    for (int i = 0; i < arr.length(); ++i) {
      if (arr.IsNull(i))
        co_yield {};
      else
        co_yield value_at(type, arr, i);
    }
  };
  if constexpr (arrow::is_extension_type<type_to_arrow_type_t<Type>>::value) {
    return impl(type, *arr.storage());
  } else {
    return impl(type, arr);
  }
}

} // namespace

auto values(const type& type,
            const std::same_as<arrow::Array> auto& array) noexcept
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

template auto values(const type& type, const arrow::Array& array) noexcept
  -> detail::generator<data_view>;

// -- utility for converting Buffer to RecordBatch -----------------------------

namespace {

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
  const FlatBuffer& slice, [[maybe_unused]] const chunk_ptr& parent,
  const std::shared_ptr<arrow::RecordBatch>& batch, type schema) noexcept
  : slice_{slice}, state_{} {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v0>) {
    VAST_ASSERT(!batch, "pre-existing record batches are only supported for "
                        "the most recent encoding");
    VAST_ASSERT(!schema, "VAST schema must be none");
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
    VAST_ASSERT(!batch, "pre-existing record batches are only supported for "
                        "the most recent encoding");
    VAST_ASSERT(!schema, "VAST schema must be none");
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
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    // We decouple the sliced type from the layout intentionally. This is an
    // absolute must because we store the state in the deletion step of the
    // table slice's chunk, and storing a sliced chunk in there would cause a
    // cyclic reference. In the future, we should just not store the sliced
    // chunk at all, but rather create it on the fly only.
    if (batch) {
      state_.record_batch = batch;
      // Technically we could infer an outer buffer here as Arrow Buffer
      // instances remember which parent buffer they were sliced from, so we if
      // know that the schema, the dictionary, and then all columns in order
      // concatenated are exactly the parent-most buffer we could get back to
      // it. This is in practice not a bottleneck, as we only create from a
      // record batch directly if we do not have the IPC backing already, so we
      // chose not to implement it and always treat the IPC backing as not yet
      // created.
      state_.is_serialized = false;
    } else {
      auto decoder = record_batch_decoder{};
      state_.record_batch = decoder.decode(
        as_arrow_buffer(parent->slice(as_bytes(*slice.arrow_ipc()))));
      state_.is_serialized = true;
    }
    if (schema) {
      state_.layout = std::move(schema);
      VAST_ASSERT(state_.layout
                  == type::from_arrow(*state_.record_batch->schema()));
    } else {
      state_.layout = type::from_arrow(*state_.record_batch->schema());
    }
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
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    if (auto&& batch = record_batch())
      return state_.flat_columns.size();
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
  return 0;
}

template <class FlatBuffer>
bool arrow_table_slice<FlatBuffer>::is_serialized() const noexcept {
  if constexpr (detail::is_any_v<FlatBuffer, fbs::table_slice::arrow::v0,
                                 fbs::table_slice::arrow::v1>) {
    return true;
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
    return state_.is_serialized;
  } else {
    static_assert(detail::always_false_v<FlatBuffer>, "unhandled arrow table "
                                                      "slice version");
  }
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
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
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
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
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
  } else if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>) {
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
                                        fbs::table_slice::arrow::v2>) {
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
                                        fbs::table_slice::arrow::v2>) {
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

// -- utility functions -------------------------------------------------------

std::pair<type, std::shared_ptr<arrow::RecordBatch>> transform_columns(
  type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
  const std::vector<indexed_transformation>& transformations) noexcept {
  VAST_ASSERT(batch->schema()->Equals(layout.to_arrow_schema()),
              "VAST layout and Arrow schema must match");
  VAST_ASSERT(std::is_sorted(transformations.begin(), transformations.end()),
              "transformations must be sorted by index");
  VAST_ASSERT(transformations.end()
                == std::adjacent_find(
                  transformations.begin(), transformations.end(),
                  [](const auto& lhs, const auto& rhs) noexcept {
                    const auto [lhs_mismatch, rhs_mismatch]
                      = std::mismatch(lhs.index.begin(), lhs.index.end(),
                                      rhs.index.begin(), rhs.index.end());
                    return lhs_mismatch == lhs.index.end();
                  }),
              "transformation indices must not be a subset of the following "
              "transformation's index");
  // The current unpacked layer of the transformation, i.e., the pieces required
  // to re-assemble the current layer of both the record type and the record
  // batch.
  struct unpacked_layer {
    std::vector<struct record_type::field> fields;
    arrow::ArrayVector arrays;
  };
  const auto impl
    = [](const auto& impl, unpacked_layer layer, offset index, auto& current,
         const auto sentinel) noexcept -> unpacked_layer {
    VAST_ASSERT(!index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer. For every entry in the current layer, we
    // need to do one of three things:
    // 1. Apply the transformation if the index matches the transformation
    //    index.
    // 2. Recurse to the next layer if the index is a prefix of the
    //    transformation index.
    // 3. Leave the elements untouched.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() noexcept -> std::pair<bool, bool> {
        if (current == sentinel)
          return {false, false};
        const auto [index_mismatch, current_index_mismatch]
          = std::mismatch(index.begin(), index.end(), current->index.begin(),
                          current->index.end());
        const auto is_prefix_match = index_mismatch == index.end();
        const auto is_exact_match
          = is_prefix_match && current_index_mismatch == current->index.end();
        return {is_prefix_match, is_exact_match};
      }();
      if (is_exact_match) {
        VAST_ASSERT(current != sentinel);
        for (auto&& [field, array] : std::invoke(
               std::move(current->fun), std::move(layer.fields[index.back()]),
               std::move(layer.arrays[index.back()]))) {
          result.fields.push_back(std::move(field));
          result.arrays.push_back(std::move(array));
        }
        ++current;
      } else if (is_prefix_match) {
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = caf::get<type_to_arrow_array_t<record_type>>(
                      *layer.arrays[index.back()])
                      .fields(),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             caf::get<record_type>(layer.fields[index.back()].type).fields())
          nested_layer.fields.push_back({std::string{name}, type});
        auto nested_index = index;
        nested_index.push_back(0);
        nested_layer = impl(impl, std::move(nested_layer),
                            std::move(nested_index), current, sentinel);
        if (!nested_layer.fields.empty()) {
          auto nested_layout = type{record_type{nested_layer.fields}};
          nested_layout.assign_metadata(layer.fields[index.back()].type);
          result.fields.emplace_back(layer.fields[index.back()].name,
                                     nested_layout);
          auto nested_arrow_fields = arrow::FieldVector{};
          nested_arrow_fields.reserve(nested_layer.fields.size());
          for (const auto& nested_field : nested_layer.fields)
            nested_arrow_fields.push_back(
              nested_field.type.to_arrow_field(nested_field.name));
          result.arrays.push_back(
            arrow::StructArray::Make(nested_layer.arrays, nested_arrow_fields)
              .ValueOrDie());
        }
      } else {
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
      }
    }
    return result;
  };
  if (transformations.empty())
    return {layout, batch};
  auto current = transformations.begin();
  const auto sentinel = transformations.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = batch->columns(),
  };
  const auto num_columns = detail::narrow_cast<size_t>(batch->num_columns());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : caf::get<record_type>(layout).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  VAST_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  VAST_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty())
    return {};
  auto new_layout = type{record_type{layer.fields}};
  new_layout.assign_metadata(layout);
  auto arrow_schema = new_layout.to_arrow_schema();
  const auto num_rows = layer.arrays[0]->length();
  return {
    std::move(new_layout),
    arrow::RecordBatch::Make(std::move(arrow_schema), num_rows,
                             std::move(layer.arrays)),
  };
}

std::pair<type, std::shared_ptr<arrow::RecordBatch>>
select_columns(type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
               const std::vector<offset>& indices) noexcept {
  VAST_ASSERT(batch->schema()->Equals(layout.to_arrow_schema()),
              "VAST layout and Arrow schema must match");
  VAST_ASSERT(std::is_sorted(indices.begin(), indices.end()), "indices must be "
                                                              "sorted");
  VAST_ASSERT(
    indices.end()
      == std::adjacent_find(indices.begin(), indices.end(),
                            [](const auto& lhs, const auto& rhs) noexcept {
                              const auto [lhs_mismatch, rhs_mismatch]
                                = std::mismatch(lhs.begin(), lhs.end(),
                                                rhs.begin(), rhs.end());
                              return lhs_mismatch == lhs.end();
                            }),
    "indices must not be a subset of the following index");
  // The current unpacked layer of the transformation, i.e., the pieces required
  // to re-assemble the current layer of both the record type and the record
  // batch.
  struct unpacked_layer {
    std::vector<struct record_type::field> fields;
    arrow::ArrayVector arrays;
  };
  const auto impl
    = [](const auto& impl, unpacked_layer layer, offset index, auto& current,
         const auto sentinel) noexcept -> unpacked_layer {
    VAST_ASSERT(!index.empty());
    auto result = unpacked_layer{};
    // Iterate over the current layer, backwards. For every entry in the current
    // layer, we need to do one of two things:
    // 1. If the indices match, keep the entry unchanged.
    // 2. Recurse to the next layer if the current index is a prefix of the
    //    selected index.
    for (; index.back() < layer.fields.size(); ++index.back()) {
      const auto [is_prefix_match, is_exact_match]
        = [&]() noexcept -> std::pair<bool, bool> {
        if (current == sentinel)
          return {false, false};
        const auto [index_mismatch, current_index_mismatch] = std::mismatch(
          index.begin(), index.end(), current->begin(), current->end());
        const auto is_prefix_match = index_mismatch == index.end();
        const auto is_exact_match
          = is_prefix_match && current_index_mismatch == current->end();
        return {is_prefix_match, is_exact_match};
      }();
      if (is_exact_match) {
        VAST_ASSERT(current != sentinel);
        result.fields.push_back(std::move(layer.fields[index.back()]));
        result.arrays.push_back(std::move(layer.arrays[index.back()]));
        ++current;
      } else if (is_prefix_match) {
        auto nested_layer = unpacked_layer{
          .fields = {},
          .arrays = caf::get<type_to_arrow_array_t<record_type>>(
                      *layer.arrays[index.back()])
                      .fields(),
        };
        nested_layer.fields.reserve(nested_layer.arrays.size());
        for (auto&& [name, type] :
             caf::get<record_type>(layer.fields[index.back()].type).fields())
          nested_layer.fields.push_back({std::string{name}, type});
        auto nested_index = index;
        nested_index.push_back(0);
        nested_layer = impl(impl, std::move(nested_layer),
                            std::move(nested_index), current, sentinel);
        auto nested_layout = type{record_type{nested_layer.fields}};
        nested_layout.assign_metadata(layer.fields[index.back()].type);
        result.fields.emplace_back(layer.fields[index.back()].name,
                                   nested_layout);
        auto nested_arrow_fields = arrow::FieldVector{};
        nested_arrow_fields.reserve(nested_layer.fields.size());
        for (const auto& nested_field : nested_layer.fields)
          nested_arrow_fields.push_back(
            nested_field.type.to_arrow_field(nested_field.name));
        result.arrays.push_back(
          arrow::StructArray::Make(nested_layer.arrays, nested_arrow_fields)
            .ValueOrDie());
      }
    }
    return result;
  };
  if (indices.empty())
    return {};
  auto current = indices.begin();
  const auto sentinel = indices.end();
  auto layer = unpacked_layer{
    .fields = {},
    .arrays = batch->columns(),
  };
  const auto num_columns = detail::narrow_cast<size_t>(batch->num_columns());
  layer.fields.reserve(num_columns);
  for (auto&& [name, type] : caf::get<record_type>(layout).fields())
    layer.fields.push_back({std::string{name}, type});
  // Run the possibly recursive implementation, starting at the last field.
  layer = impl(impl, std::move(layer), {0}, current, sentinel);
  VAST_ASSERT(current == sentinel, "index out of bounds");
  // Re-assemble the record batch after the transformation.
  VAST_ASSERT(layer.fields.size() == layer.arrays.size());
  if (layer.fields.empty())
    return {};
  auto new_layout = type{record_type{layer.fields}};
  new_layout.assign_metadata(layout);
  auto arrow_schema = new_layout.to_arrow_schema();
  const auto num_rows = layer.arrays[0]->length();
  return {
    std::move(new_layout),
    arrow::RecordBatch::Make(std::move(arrow_schema), num_rows,
                             std::move(layer.arrays)),
  };
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all Arrow encoding versions.
template class arrow_table_slice<fbs::table_slice::arrow::v0>;
template class arrow_table_slice<fbs::table_slice::arrow::v1>;
template class arrow_table_slice<fbs::table_slice::arrow::v2>;

// -- utility functions --------------------------------------------------------

namespace {

arrow::Result<std::shared_ptr<arrow::Array>>
convert_subnet_array(const arrow::FixedSizeBinaryArray& arr) {
  auto* pool = arrow::default_memory_pool();
  auto subnet_builder = subnet_type::make_arrow_builder(pool);
  auto& address_builder = subnet_builder->address_builder();
  auto& length_builder = subnet_builder->length_builder();
  ARROW_RETURN_NOT_OK(address_builder.Resize(arr.length()));
  ARROW_RETURN_NOT_OK(length_builder.Resize(arr.length()));
  ARROW_RETURN_NOT_OK(subnet_builder->Resize(arr.length()));
  for (int row = 0; row < arr.length(); ++row) {
    if (arr.IsNull(row)) {
      ARROW_RETURN_NOT_OK(subnet_builder->AppendNull());
    } else {
      auto span = std::span<const uint8_t, 17>{arr.GetValue(row), 17};
      ARROW_RETURN_NOT_OK(subnet_builder->Append());
      ARROW_RETURN_NOT_OK(address_builder.Append(arr.GetValue(row)));
      ARROW_RETURN_NOT_OK(length_builder.Append(span[16]));
    }
  }
  if (auto result = subnet_builder->Finish(); result.ok())
    return *result;
  die("failed to finish Arrow subnet column builder");
}

/// Converts an array representing a single column from a previous arrow
/// format to the according representation in the current arrow format.
std::shared_ptr<arrow::Array>
upgrade_array_to_v2(const std::shared_ptr<arrow::Array>& arr, const type& t) {
  auto f = detail::overload{
    [&](const basic_type auto& t) -> std::shared_ptr<arrow::Array> {
      VAST_ASSERT(arr->type()->Equals(t.to_arrow_type()));
      return arr;
    },
    [&](const duration_type&) -> std::shared_ptr<arrow::Array> {
      // Duration is backed by physical Int64 array, but logical type differs.
      auto result = arr->View(duration_type::to_arrow_type());
      VAST_ASSERT(result.ok(), result.status().ToString().c_str());
      return result.MoveValueUnsafe();
    },
    [&](const record_type& rt) -> std::shared_ptr<arrow::Array> {
      // this case handles VAST type `list<record>`
      const auto& sa = static_pointer_cast<arrow::StructArray>(arr);
      arrow::ArrayVector children{};
      std::vector<std::string> field_names{};
      children.reserve(rt.num_fields());
      field_names.reserve(rt.num_fields());
      int index = 0;
      for (const auto& f : rt.fields()) {
        const auto& arr = sa->field(index++);
        children.push_back(upgrade_array_to_v2(arr, f.type));
        field_names.emplace_back(f.name);
      }
      auto res = arrow::StructArray::Make(children, field_names);
      if (!res.ok())
        die("unable to construct nested struct array");
      return res.MoveValueUnsafe();
    },
    [&](const pattern_type&) -> std::shared_ptr<arrow::Array> {
      return std::make_shared<pattern_type::array_type>(
        pattern_type::to_arrow_type(), arr);
    },
    [&](const address_type&) -> std::shared_ptr<arrow::Array> {
      return std::make_shared<address_type::array_type>(
        address_type::to_arrow_type(), arr);
    },
    [&](const subnet_type&) -> std::shared_ptr<arrow::Array> {
      auto ba = static_pointer_cast<arrow::FixedSizeBinaryArray>(arr);
      VAST_ASSERT(ba->byte_width() == 17);
      auto subnet_array = convert_subnet_array(*ba);
      if (!subnet_array.ok())
        die("failed building subnet array from fixedsizebinary(17)");
      return subnet_array.MoveValueUnsafe();
    },
    [&](const enumeration_type& et) -> std::shared_ptr<arrow::Array> {
      auto indices = arrow::compute::Cast(*arr, arrow::uint8());
      if (!indices.ok())
        die("failed casting int64 to uint8");
      auto enum_value = enumeration_type::array_type::make(
        et.to_arrow_type(),
        static_pointer_cast<arrow::UInt8Array>(indices.MoveValueUnsafe()));
      if (!enum_value.ok())
        die("failed constructing extension type array");
      return enum_value.MoveValueUnsafe();
    },
    [&](const list_type& lt) -> std::shared_ptr<arrow::Array> {
      const auto& la = static_cast<const arrow::ListArray&>(*arr);
      const auto& inner = upgrade_array_to_v2(la.values(), lt.value_type());
      const auto& list_array = std::make_shared<arrow::ListArray>(
        arrow::list(inner->type()), la.length(), la.value_offsets(), inner,
        la.null_bitmap(), la.null_count());
      return list_array;
    },
    [&](const map_type& mt) -> std::shared_ptr<arrow::Array> {
      const auto& la = static_cast<const arrow::ListArray&>(*arr);
      const auto& structs
        = static_cast<const arrow::StructArray&>(*la.values());
      const auto& keys = upgrade_array_to_v2(structs.field(0), mt.key_type());
      const auto& items
        = upgrade_array_to_v2(structs.field(1), mt.value_type());
      // Workaround to a segfault we encountered with arrow 9.0.0: Calling
      // `null_count()` has the side effect of initializing the `null_count`
      // variable from -1 to its correct value, and the constructor of
      // `MapArray` asserts that `null_count != 0`.
      keys->null_count();
      return std::make_shared<arrow::MapArray>(mt.to_arrow_type(), la.length(),
                                               la.value_offsets(), keys, items,
                                               la.null_bitmap(),
                                               la.null_count());
    },
  };
  return caf::visit(f, t);
}

/// Consumes arrow arrays and builds up the appropriate array based on the
/// provided data type.
/// @param array_iterator An iterator to consume column arrays.
/// @param arrow_type the data type we're building the array for.
std::shared_ptr<arrow::Array>
make_arrow_array(arrow::ArrayVector::const_iterator& array_iterator,
                 const type& t) {
  auto f = detail::overload{
    [&](const auto& t) -> std::shared_ptr<arrow::Array> {
      return upgrade_array_to_v2(*array_iterator++, type{t});
    },
    [&](const record_type& rt) -> std::shared_ptr<arrow::Array> {
      arrow::ArrayVector children{};
      std::vector<std::string> field_names{};
      children.reserve(rt.num_fields());
      field_names.reserve(rt.num_fields());
      for (const auto& field : rt.fields()) {
        children.push_back(make_arrow_array(array_iterator, field.type));
        field_names.emplace_back(field.name);
      }
      auto res = arrow::StructArray::Make(children, field_names);
      return res.ValueOrDie();
    },
  };
  return caf::visit(f, t);
}

} // namespace

std::shared_ptr<arrow::RecordBatch>
convert_record_batch(const std::shared_ptr<arrow::RecordBatch>& legacy,
                     const type& t) {
  const auto& schema = t.to_arrow_schema();
  VAST_ASSERT(caf::holds_alternative<record_type>(t));
  const auto* rt = caf::get_if<record_type>(&t);
  auto it = legacy->columns().cbegin();
  auto output_columns = arrow::ArrayVector{};
  output_columns.reserve(schema->num_fields());
  for (const auto& field : rt->fields())
    output_columns.push_back(make_arrow_array(it, field.type));
  return arrow::RecordBatch::Make(schema, legacy->num_rows(),
                                  std::move(output_columns));
}

} // namespace vast
