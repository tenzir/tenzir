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
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <type_traits>
#include <utility>

namespace vast {

namespace {

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
#define DECODE_TRY_DISPATCH(vast_type)                                         \
  if (auto dt = caf::get_if<vast_type##_type>(&t))                             \
  return f(arr, *dt)

template <class F>
void decode(const type& t, const arrow::BooleanArray& arr, F& f) {
  DECODE_TRY_DISPATCH(bool);
  VAST_WARN("{} expected to decode a boolean but got a {}", __func__, t);
}

template <class T, class F>
void decode(const type& t, const arrow::NumericArray<T>& arr, F& f) {
  if constexpr (arrow::is_floating_type<T>::value) {
    DECODE_TRY_DISPATCH(real);
    VAST_WARN("{} expected to decode a real but got a {}", __func__, t);
  } else if constexpr (std::is_signed_v<typename T::c_type>) {
    DECODE_TRY_DISPATCH(integer);
    DECODE_TRY_DISPATCH(duration);
    VAST_WARN("{} expected to decode an integer or timespan but got a "
              "{}",
              __func__, t);
  } else {
    DECODE_TRY_DISPATCH(count);
    DECODE_TRY_DISPATCH(enumeration);
    VAST_WARN("{} expected to decode a count or enumeration but got a "
              "{}",
              __func__, t);
  }
}

template <class F>
void decode(const type& t, const arrow::FixedSizeBinaryArray& arr, F& f) {
  DECODE_TRY_DISPATCH(address);
  DECODE_TRY_DISPATCH(subnet);
  VAST_WARN("{} expected to decode an address or subnet but got a {}", __func__,
            t);
}

template <class F>
void decode(const type& t, const arrow::StringArray& arr, F& f) {
  DECODE_TRY_DISPATCH(string);
  DECODE_TRY_DISPATCH(pattern);
  VAST_WARN("{} expected to decode a string or pattern but got a {}", __func__,
            t);
}

template <class F>
void decode(const type& t, const arrow::TimestampArray& arr, F& f) {
  DECODE_TRY_DISPATCH(time);
  VAST_WARN("{} expected to decode a timestamp but got a {}", __func__, t);
}

template <class F>
void decode(const type& t, const arrow::ListArray& arr, F& f) {
  DECODE_TRY_DISPATCH(list);
  DECODE_TRY_DISPATCH(map);
  VAST_WARN("{} expected to decode a list or map but got a {}", __func__, t);
}

template <class F>
void decode(const type& t, const arrow::StructArray& arr, F& f) {
  DECODE_TRY_DISPATCH(record);
  VAST_WARN("{} expected to decode a record but got a {}", __func__, t);
}

template <class F>
void decode(const type& t, const arrow::Array& arr, F& f) {
  switch (arr.type_id()) {
    default: {
      VAST_WARN("{} got an unrecognized Arrow type ID", __func__);
      break;
    }
    // -- handle basic types ---------------------------------------------------
    case arrow::Type::BOOL: {
      return decode(t, static_cast<const arrow::BooleanArray&>(arr), f);
    }
    case arrow::Type::STRING: {
      return decode(t, static_cast<const arrow::StringArray&>(arr), f);
    }
    case arrow::Type::TIMESTAMP: {
      return decode(t, static_cast<const arrow::TimestampArray&>(arr), f);
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      using array_type = arrow::FixedSizeBinaryArray;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    // -- handle container types -----------------------------------------------
    case arrow::Type::LIST: {
      return decode(t, static_cast<const arrow::ListArray&>(arr), f);
    }
    case arrow::Type::STRUCT: {
      return decode(t, static_cast<const arrow::StructArray&>(arr), f);
    }
    // -- lift floating point values to real -----------------------------
    case arrow::Type::HALF_FLOAT: {
      using array_type = arrow::NumericArray<arrow::HalfFloatType>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::FLOAT: {
      using array_type = arrow::NumericArray<arrow::FloatType>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::DOUBLE: {
      using array_type = arrow::NumericArray<arrow::DoubleType>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    // -- lift singed values to integer ----------------------------------
    case arrow::Type::INT8: {
      using array_type = arrow::NumericArray<arrow::Int8Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::INT16: {
      using array_type = arrow::NumericArray<arrow::Int16Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::INT32: {
      using array_type = arrow::NumericArray<arrow::Int32Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::INT64: {
      using array_type = arrow::NumericArray<arrow::Int64Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    // -- lift unsinged values to count ----------------------------------
    case arrow::Type::UINT8: {
      using array_type = arrow::NumericArray<arrow::UInt8Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::UINT16: {
      using array_type = arrow::NumericArray<arrow::UInt16Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::UINT32: {
      using array_type = arrow::NumericArray<arrow::UInt32Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
    case arrow::Type::UINT64: {
      using array_type = arrow::NumericArray<arrow::UInt64Type>;
      return decode(t, static_cast<const array_type&>(arr), f);
    }
  }
}

#undef DECODE_TRY_DISPATCH

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
  return address::v6(static_cast<const void*>(bytes), address::network);
}

auto subnet_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 17);
  auto addr = address::v6(static_cast<const void*>(bytes), address::network);
  return subnet{addr, bytes[16]};
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
  type kvp_type = record_type{
    {"key", std::move(key_type)},
    {"value", std::move(value_type)},
  };
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
      using data_type = type_to_data_t<U>;
      result_ = static_cast<data_type>(arr.Value(row_));
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

// -- utility for converting Buffer to RecordBatch -----------------------------

template <class Callback>
class record_batch_listener final : public arrow::ipc::Listener {
public:
  record_batch_listener(Callback&& callback) noexcept
    : callback_{std::forward<Callback>(callback)} {
    // nop
  }

  virtual ~record_batch_listener() noexcept override = default;

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

  template <class FlatSchema, class FlatRecordBatch>
  std::shared_ptr<arrow::RecordBatch>
  decode(const FlatSchema& flat_schema,
         const FlatRecordBatch& flat_record_batch) noexcept {
    VAST_ASSERT(!record_batch_);
    if (auto status
        = decoder_.Consume(flat_schema->data(), flat_schema->size());
        !status.ok()) {
      VAST_ERROR("{} failed to decode Arrow Schema: {}", __func__,
                 status.ToString());
      return {};
    }
    if (auto status = decoder_.Consume(flat_record_batch->data(),
                                       flat_record_batch->size());
        !status.ok()) {
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

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::arrow_table_slice(
  const FlatBuffer& slice, const chunk_ptr& parent) noexcept
  : slice_{slice}, state_{} {
  if constexpr (std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v0>) {
    // This legacy type has to stay; it is deserialized from disk.
    auto intermediate = legacy_record_type{};
    if (auto err = fbs::deserialize_bytes(slice_.layout(), intermediate))
      die("failed to deserialize layout: " + render(err));
    state_.layout = type::from_legacy_type(intermediate);
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.decode(slice.schema(), slice.record_batch());
  } else {
    state_.layout = type{parent->slice(as_bytes(*slice_.layout()))};
    VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
    auto decoder = record_batch_decoder{};
    state_.record_batch = decoder.decode(slice.schema(), slice.record_batch());
  }
}

template <class FlatBuffer>
arrow_table_slice<FlatBuffer>::~arrow_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

template <class FlatBuffer>
const record_type& arrow_table_slice<FlatBuffer>::layout() const noexcept {
  return caf::get<record_type>(state_.layout);
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::rows() const noexcept {
  if (auto&& batch = record_batch())
    return batch->num_rows();
  return 0;
}

template <class FlatBuffer>
table_slice::size_type arrow_table_slice<FlatBuffer>::columns() const noexcept {
  if (auto&& batch = record_batch())
    return batch->num_columns();
  return 0;
}

// -- data access ------------------------------------------------------------

template <class FlatBuffer>
void arrow_table_slice<FlatBuffer>::append_column_to_index(
  id offset, table_slice::size_type column, value_index& index) const {
  if (auto&& batch = record_batch()) {
    auto f = index_applier{offset, index};
    auto array = batch->column(detail::narrow_cast<int>(column));
    auto offset = this->layout().resolve_flat_index(column);
    decode(this->layout().field(offset).type, *array, f);
  }
}

template <class FlatBuffer>
data_view
arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                  table_slice::size_type column) const {
  auto&& batch = record_batch();
  VAST_ASSERT(batch);
  auto array = batch->column(detail::narrow_cast<int>(column));
  auto offset = this->layout().resolve_flat_index(column);
  return value_at(this->layout().field(offset).type, *array, row);
}

template <class FlatBuffer>
data_view arrow_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                            table_slice::size_type column,
                                            const type& t) const {
  VAST_ASSERT(congruent(
    this->layout().field(this->layout().resolve_flat_index(column)).type, t));
  auto&& batch = record_batch();
  VAST_ASSERT(batch);
  auto array = batch->column(detail::narrow_cast<int>(column));
  return value_at(t, *array, row);
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

} // namespace vast
