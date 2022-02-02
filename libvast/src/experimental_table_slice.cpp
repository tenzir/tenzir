//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/experimental_table_slice.hpp"

#include "vast/arrow_extension_types.hpp"
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
#include <arrow/ipc/api.h>

#include <type_traits>
#include <utility>

namespace vast {

namespace {

// -- utility class for mapping Arrow lists to VAST container views ------------

template <class Array>
data_view value_at(const type& t, const Array& arr, size_t row);

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

template <>
struct decodable<duration_type, arrow::DurationArray> : std::true_type {};

template <class TypeClass>
  requires(
    std::is_integral_v<
      typename TypeClass::c_type> && !std::is_signed_v<typename TypeClass::c_type>)
struct decodable<count_type, arrow::NumericArray<TypeClass>> : std::true_type {
};

template <>
struct decodable<enumeration_type, arrow::DictionaryArray> : std::true_type {};

template <>
struct decodable<address_type, arrow::FixedSizeBinaryArray> : std::true_type {};

template <>
struct decodable<subnet_type, arrow::StructArray> : std::true_type {};

template <>
struct decodable<string_type, arrow::StringArray> : std::true_type {};

template <>
struct decodable<pattern_type, arrow::StringArray> : std::true_type {};

template <>
struct decodable<time_type, arrow::TimestampArray> : std::true_type {};

template <>
struct decodable<list_type, arrow::ListArray> : std::true_type {};

template <>
struct decodable<map_type, arrow::MapArray> : std::true_type {};

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
      VAST_WARN("{} got unrecognized Arrow type ID '{}' ({})", __func__,
                arr.type_id(), arr.type()->ToString());
      return;
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
    case arrow::Type::DURATION: {
      return dispatch(static_cast<const arrow::DurationArray&>(arr));
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      using array_type = arrow::FixedSizeBinaryArray;
      return dispatch(static_cast<const array_type&>(arr));
    }
    // -- handle container types -----------------------------------------------
    case arrow::Type::MAP: {
      return dispatch(static_cast<const arrow::MapArray&>(arr));
    }
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
    // -- lift signed values to integer ----------------------------------
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
    case arrow::Type::EXTENSION: {
      const auto& t = static_cast<const arrow::ExtensionType&>(*arr.type());
      const auto& ext_arr = static_cast<const arrow::ExtensionArray&>(arr);
      if (t.extension_name() == enum_extension_type::vast_id)
        return dispatch(
          static_cast<const arrow::DictionaryArray&>(*ext_arr.storage()));
      if (t.extension_name() == address_extension_type::vast_id)
        return dispatch(
          static_cast<const arrow::FixedSizeBinaryArray&>(*ext_arr.storage()));
      if (t.extension_name() == subnet_extension_type::vast_id)
        return dispatch(
          static_cast<const arrow::StructArray&>(*ext_arr.storage()));
      if (t.extension_name() == pattern_extension_type::vast_id)
        return dispatch(
          static_cast<const arrow::StringArray&>(*ext_arr.storage()));
      die(fmt::format("Unable to handle extension type '{}'",
                      t.extension_name()));
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

auto pattern_at(const arrow::StringArray& arr, int64_t row) {
  return pattern_view{string_at(arr, row)};
}

auto address_at(const arrow::FixedSizeBinaryArray& arr, int64_t row) {
  auto bytes = arr.raw_values() + (row * 16);
  auto span = std::span<const uint8_t, 16>{bytes, 16};
  return address::v6(span);
}

auto subnet_at(const arrow::StructArray& arr, int64_t row) {
  const auto& length_array = arr.field(0);
  const auto& ext_arr
    = static_pointer_cast<arrow::ExtensionArray>(arr.field(1));
  const auto& address_array = *ext_arr->storage();
  auto addr = address_at(
    static_cast<const arrow::FixedSizeBinaryArray&>(address_array), row);
  auto len
    = count_at(static_cast<const arrow::UInt8Array&>(*length_array), row);
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

auto map_at(type key_type, type value_type, const arrow::ListArray& arr,
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
    using view_type = view<type_to_data_t<U>>;
    result_ = static_cast<view_type>(arr.Value(row_));
  }

  void operator()(const arrow::DictionaryArray& arr, const enumeration_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = enumeration_at(arr, row_);
  }

  void operator()(const arrow::DurationArray& arr, const duration_type&) {
    if (arr.IsNull(row_))
      return;
    result_ = duration_at(arr, row_);
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

  void operator()(const arrow::StructArray& arr, const subnet_type&) {
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

  void operator()(const arrow::ListArray& arr, const list_type& t) {
    if (arr.IsNull(row_))
      return;
    result_ = list_at(t.value_type(), arr, row_);
  }

  void operator()(const arrow::MapArray& arr, const map_type& t) {
    if (arr.IsNull(row_))
      return;
    result_ = map_at(t.key_type(), t.value_type(), arr, row_);
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

  void operator()(const arrow::DictionaryArray& arr, const enumeration_type&) {
    apply(arr, enumeration_at);
  }

  void operator()(const arrow::DurationArray& arr, const duration_type&) {
    apply(arr, duration_at);
  }

  void operator()(const arrow::FixedSizeBinaryArray& arr, const address_type&) {
    apply(arr, address_at);
  }

  void operator()(const arrow::StructArray& arr, const subnet_type&) {
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

  void operator()(const arrow::ListArray& arr, const list_type& t) {
    apply(arr, [&](const auto& arr, int64_t row) {
      return list_at(t.value_type(), arr, row);
    });
  }

  void operator()(const arrow::MapArray& arr, const map_type& t) {
    apply(arr, [&](const auto& arr, int64_t row) {
      return map_at(t.key_type(), t.value_type(), arr, row);
    });
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

  ~record_batch_listener() noexcept override = default;

private:
  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    std::invoke(callback_, std::move(record_batch));
    return arrow::Status::OK();
  }

  // TODO: can we already derive (and VASTify) the schema at this stage?
  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema>) override {
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

void index_column_arrays(const std::shared_ptr<arrow::Array>& arr,
                         arrow::ArrayVector& out) {
  auto f = detail::overload{[&](const std::shared_ptr<arrow::Array>& a) {
                              out.push_back(a);
                            },
                            [&](const std::shared_ptr<arrow::StructArray>& s) {
                              for (const auto& child : s->fields())
                                index_column_arrays(child, out);
                            }};
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

experimental_table_slice::experimental_table_slice(
  const fbs::table_slice::arrow::experimental& slice,
  [[maybe_unused]] const chunk_ptr& parent) noexcept
  : slice_{slice}, state_{} {
  // We decouple the sliced type from the layout intentionally. This is an
  // absolute must because we store the state in the deletion step of the
  // table slice's chunk, and storing a sliced chunk in there would cause a
  // cyclic reference. In the future, we should just not store the sliced
  // chunk at all, but rather create it on the fly only.
  state_.layout = type{chunk::copy(as_bytes(*slice_.layout()))};
  VAST_ASSERT(caf::holds_alternative<record_type>(state_.layout));
  auto decoder = record_batch_decoder{};
  state_.record_batch = decoder.decode(
    as_arrow_buffer(parent->slice(as_bytes(*slice.arrow_ipc()))));
  state_.array_index = index_column_arrays(state_.record_batch);
}

experimental_table_slice::~experimental_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

const type& experimental_table_slice::layout() const noexcept {
  return state_.layout;
}

table_slice::size_type experimental_table_slice::rows() const noexcept {
  if (auto&& batch = record_batch())
    return batch->num_rows();
  return 0;
}

table_slice::size_type experimental_table_slice::columns() const noexcept {
  if (auto&& batch = record_batch())
    return state_.array_index.size();
  return 0;
}

// -- data access ------------------------------------------------------------

void experimental_table_slice::append_column_to_index(
  id offset, table_slice::size_type column, value_index& index) const {
  if (auto&& batch = record_batch()) {
    auto f = index_applier{offset, index};
    auto array = this->column_array(column);
    const auto& layout = caf::get<record_type>(this->layout());
    auto offset = layout.resolve_flat_index(column);
    decode(layout.field(offset).type, *array, f);
  }
}

data_view experimental_table_slice::at(table_slice::size_type row,
                                       table_slice::size_type column) const {
  auto&& array = this->column_array(column);
  const auto& layout = caf::get<record_type>(this->layout());
  auto offset = layout.resolve_flat_index(column);
  return value_at(layout.field(offset).type, *array, row);
}

data_view experimental_table_slice::at(table_slice::size_type row,
                                       table_slice::size_type column,
                                       const type& t) const {
  VAST_ASSERT(congruent(
    caf::get<record_type>(this->layout())
      .field(caf::get<record_type>(this->layout()).resolve_flat_index(column))
      .type,
    t));
  auto&& array = this->column_array(column);
  return value_at(t, *array, row);
}

time experimental_table_slice::import_time() const noexcept {
  return time{} + duration{slice_.import_time()};
}

void experimental_table_slice::import_time(time import_time) noexcept {
  auto result = const_cast<fbs::table_slice::arrow::experimental&>(slice_)
                  .mutate_import_time(import_time.time_since_epoch().count());
  VAST_ASSERT(result, "failed to mutate import time");
}

std::shared_ptr<arrow::RecordBatch>
experimental_table_slice::record_batch() const noexcept {
  return state_.record_batch;
}

std::shared_ptr<arrow::Array> experimental_table_slice::column_array(
  table_slice::size_type column) const noexcept {
  return state_.array_index[column];
}

} // namespace vast
