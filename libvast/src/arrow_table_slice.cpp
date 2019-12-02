/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/arrow_table_slice.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/detail/type_list.hpp>

#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

namespace vast {

arrow_table_slice::arrow_table_slice(table_slice_header header,
                                     record_batch_ptr batch)
  : super(std::move(header)), batch_(std::move(batch)) {
  // nop
}

table_slice_ptr arrow_table_slice::make(table_slice_header header) {
  return table_slice_ptr{new arrow_table_slice(std::move(header))};
}

arrow_table_slice* arrow_table_slice::copy() const {
  return new arrow_table_slice(header_, batch_);
}

namespace {

class arrow_output_stream : public arrow::io::OutputStream {
public:
  arrow_output_stream(caf::binary_serializer& sink) : sink_(sink) {
    // nop
  }

  bool closed() const override {
    return false;
  }

  arrow::Status Close() override {
    return arrow::Status::OK();
  }

  arrow::Status Tell(int64_t* position) const override {
    VAST_ASSERT(position != nullptr);
    *position = detail::narrow_cast<int64_t>(sink_.write_pos());
    return arrow::Status::OK();
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    sink_.apply_raw(detail::narrow_cast<size_t>(nbytes),
                    const_cast<void*>(data));
    return arrow::Status::OK();
  }

private:
  caf::binary_serializer& sink_;
};

class arrow_input_stream : public arrow::io::InputStream {
public:
  arrow_input_stream(caf::deserializer& source)
    : source_(source), position_(0) {
    // nop
  }

  bool closed() const override {
    return false;
  }

  arrow::Status Close() override {
    return arrow::Status::OK();
  }

  arrow::Status Tell(int64_t* position) const override {
    if (position != nullptr)
      *position = position_;
    return arrow::Status::OK();
  }

  arrow::Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override {
    if (auto err = source_.apply_raw(detail::narrow_cast<size_t>(nbytes), out))
      return arrow::Status::IOError("Past end of stream");
    if (bytes_read)
      *bytes_read = nbytes;
    return arrow::Status::OK();
  }

  arrow::Status
  Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override {
    VAST_ASSERT(out != nullptr);
    if (auto code = arrow::AllocateBuffer(nbytes, out); !code.ok())
      return code;
    VAST_ASSERT(out != nullptr);
    return Read(nbytes, nullptr,
                reinterpret_cast<void*>((*out)->mutable_data()));
  }

private:
  caf::deserializer& source_;
  int64_t position_;
};

} // namespace

caf::error arrow_table_slice::serialize(caf::serializer& sink) const {
  if (auto derived = dynamic_cast<caf::binary_serializer*>(&sink))
    return serialize_impl(*derived);
  std::vector<char> buf;
  caf::binary_serializer binary_sink{nullptr, buf};
  if (auto err = serialize_impl(binary_sink))
    return err;
  return sink.apply_raw(buf.size(), buf.data());
}

caf::error
arrow_table_slice::serialize_impl(caf::binary_serializer& sink) const {
  arrow_output_stream output_stream{sink};
  if (rows() == 0)
    return caf::none;
  VAST_ASSERT(batch_ != nullptr);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto schema = batch_->schema();
  auto st = arrow::ipc::RecordBatchStreamWriter::Open(&output_stream, schema,
                                                      &writer);
  if (!st.ok())
    return ec::unspecified;
  if (!writer->WriteRecordBatch(*batch_).ok())
    return ec::unspecified;
  return caf::none;
}

caf::error arrow_table_slice::deserialize(caf::deserializer& source) {
  arrow_input_stream input_stream{source};
  if (rows() == 0) {
    batch_ = nullptr;
    return caf::none;
  }
  std::shared_ptr<arrow::RecordBatchReader> reader;
  auto st = arrow::ipc::RecordBatchStreamReader::Open(&input_stream, &reader);
  if (!st.ok())
    return ec::unspecified;
  if (!reader->ReadNext(&batch_).ok())
    return ec::unspecified;
  return caf::none;
}

caf::atom_value arrow_table_slice::implementation_id() const noexcept {
  return class_id;
}

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
      if (auto dt = caf::get_if<record_type>(&element_type_)) {
        if (dt->fields.size() == 2) {
          auto& arr = static_cast<const arrow::StructArray&>(*arr_);
          auto key_arr = arr.field(0);
          auto value_arr = arr.field(1);
          return {value_at(dt->fields[0].type, *key_arr, adjusted_row),
                  value_at(dt->fields[1].type, *value_arr, adjusted_row)};
        }
      }
      return {caf::none, caf::none};
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

// -- decoding of Arrow column arrays ------------------------------------------

// Safe ourselves redundant boilerplate code for dispatching to the visitor.
#define DECODE_TRY_DISPATCH(vast_type)                                         \
  if (auto dt = caf::get_if<vast_type##_type>(&t))                             \
  return f(arr, *dt)

template <class F>
void decode(const type& t, const arrow::BooleanArray& arr, F& f) {
  DECODE_TRY_DISPATCH(bool);
  VAST_WARNING(__func__, "expected to decode a boolean but got a", kind(t));
}

template <class T, class F>
void decode(const type& t, const arrow::NumericArray<T>& arr, F& f) {
  if constexpr (arrow::is_floating_type<T>::value) {
    DECODE_TRY_DISPATCH(real);
    VAST_WARNING(__func__, "expected to decode a real but got a", kind(t));
  } else if constexpr (std::is_signed_v<typename T::c_type>) {
    DECODE_TRY_DISPATCH(integer);
    DECODE_TRY_DISPATCH(duration);
    VAST_WARNING(
      __func__, "expected to decode an integer or timespan but got a", kind(t));
  } else {
    DECODE_TRY_DISPATCH(count);
    DECODE_TRY_DISPATCH(enumeration);
    VAST_WARNING(
      __func__, "expected to decode a count or enumeration but got a", kind(t));
  }
}

template <class F>
void decode(const type& t, const arrow::FixedSizeBinaryArray& arr, F& f) {
  DECODE_TRY_DISPATCH(address);
  DECODE_TRY_DISPATCH(subnet);
  DECODE_TRY_DISPATCH(port);
  VAST_WARNING(__func__,
               "expected to decode an address, subnet, or port but got a",
               kind(t));
}

template <class F>
void decode(const type& t, const arrow::StringArray& arr, F& f) {
  DECODE_TRY_DISPATCH(string);
  DECODE_TRY_DISPATCH(pattern);
  VAST_WARNING(__func__, "expected to decode a string or pattern but got a",
               kind(t));
}

template <class F>
void decode(const type& t, const arrow::TimestampArray& arr, F& f) {
  DECODE_TRY_DISPATCH(time);
  VAST_WARNING(__func__, "expected to decode a timestamp but got a", kind(t));
}

template <class F>
void decode(const type& t, const arrow::ListArray& arr, F& f) {
  DECODE_TRY_DISPATCH(vector);
  DECODE_TRY_DISPATCH(set);
  DECODE_TRY_DISPATCH(map);
  VAST_WARNING(__func__, "expected to decode a list, set, or map but got a",
               kind(t));
}

template <class F>
void decode(const type& t, const arrow::Array& arr, F& f) {
  switch (arr.type_id()) {
    default: {
      VAST_WARNING(__func__, "got an unrecognized Arrow type ID");
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
  return static_cast<integer>(arr.Value(row));
};

auto count_at = [](const auto& arr, int64_t row) {
  return static_cast<count>(arr.Value(row));
};

auto enumeration_at = [](const auto& arr, int64_t row) {
  return static_cast<enumeration>(arr.Value(row));
};

auto duration_at
  = [](const auto& arr, int64_t row) { return duration{arr.Value(row)}; };

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

auto port_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 3);
  uint8_t n[2] = {bytes[0], bytes[1]};
  return port(*reinterpret_cast<uint16_t*>(n),
              static_cast<port::port_type>(bytes[2]));
}

auto timestamp_at(const arrow::TimestampArray& arr, int64_t row) {
  auto ts_value = static_cast<integer>(arr.Value(row));
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

auto vector_at(type value_type, const arrow::ListArray& arr, int64_t row) {
  auto ptr = container_view_at(std::move(value_type), arr, row);
  return vector_view_handle{vector_view_ptr{std::move(ptr)}};
}

auto set_at(type value_type, const arrow::ListArray& arr, int64_t row) {
  auto ptr = container_view_at(std::move(value_type), arr, row);
  return set_view_handle{set_view_ptr{std::move(ptr)}};
}

auto map_at(type key_type, type value_type, const arrow::ListArray& arr,
            int64_t row) {
  using view_impl = arrow_container_view<std::pair<data_view, data_view>>;
  auto offset = arr.value_offset(row);
  auto length = arr.value_length(row);
  type kvp_type = record_type{{"key", std::move(key_type)},
                              {"value", std::move(value_type)}};
  auto ptr = caf::make_counted<view_impl>(std::move(kvp_type), arr.values(),
                                          offset, length);
  return map_view_handle{map_view_ptr{std::move(ptr)}};
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
      using data_type = typename type_traits<U>::data_type;
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

  void operator()(const arrow::FixedSizeBinaryArray& arr, const port_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = port_at(arr, row_);
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
    if constexpr (std::is_same_v<T, vector_type>) {
      result_ = vector_at(t.value_type, arr, row_);
    } else if constexpr (std::is_same_v<T, set_type>) {
      result_ = set_at(t.value_type, arr, row_);
    } else {
      static_assert(std::is_same_v<T, map_type>);
      result_ = map_at(t.key_type, t.value_type, arr, row_);
    }
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

  void operator()(const arrow::FixedSizeBinaryArray& arr, const port_type&) {
    apply(arr, port_at);
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
    if constexpr (std::is_same_v<T, vector_type>) {
      auto f = [&](const auto& arr, int64_t row) {
        return vector_at(t.value_type, arr, row);
      };
      apply(arr, f);
    } else if constexpr (std::is_same_v<T, set_type>) {
      auto f = [&](const auto& arr, int64_t row) {
        return set_at(t.value_type, arr, row);
      };
      apply(arr, f);
    } else {
      static_assert(std::is_same_v<T, map_type>);
      auto f = [&](const auto& arr, int64_t row) {
        return map_at(t.key_type, t.value_type, arr, row);
      };
      apply(arr, f);
    }
  }

private:
  int64_t offset_;
  value_index& idx_;
};

} // namespace

// -- remaining implementation of arrow_table_slice ----------------------------

data_view arrow_table_slice::at(size_type row, size_type col) const {
  VAST_ASSERT(row < rows());
  VAST_ASSERT(col < columns());
  auto arr = batch_->column(detail::narrow_cast<int>(col));
  return value_at(layout().fields[col].type, *arr, row);
}

void arrow_table_slice::append_column_to_index(size_type col,
                                               value_index& idx) const {
  index_applier f{offset(), idx};
  auto arr = batch_->column(detail::narrow_cast<int>(col));
  decode(layout().fields[col].type, *arr, f);
}

} // namespace vast
