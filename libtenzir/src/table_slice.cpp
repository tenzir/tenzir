//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/table_slice.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/bitmap_algorithms.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/fbs/table_slice.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/replace_columns.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

#include <arrow/compute/cast.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include <cstddef>
#include <ranges>
#include <span>
#include <string_view>

namespace tenzir {

// -- utility functions --------------------------------------------------------

namespace {

/// Create a table slice from a record batch.
/// @param record_batch The record batch to encode.
/// @param builder The flatbuffers builder to use.
/// @param serialize Embed the IPC format in the FlatBuffers table.
table_slice
create_table_slice(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                   flatbuffers::FlatBufferBuilder& builder, type schema,
                   table_slice::serialize serialize) {
  TENZIR_ASSERT(record_batch);
#if TENZIR_ENABLE_ASSERTIONS
  // NOTE: There's also a ValidateFull function, but that always errors when
  // using nested struct arrays. Last tested with Arrow 7.0.0. -- DL.
  auto validate_status = record_batch->Validate();
  TENZIR_ASSERT_EXPENSIVE(validate_status.ok(),
                          validate_status.ToString().c_str());
#endif // TENZIR_ENABLE_ASSERTIONS
  auto fbs_ipc_buffer = flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{};
  if (serialize == table_slice::serialize::yes) {
    auto ipc_ostream = check(
      arrow::io::BufferOutputStream::Create(4096, tenzir::arrow_memory_pool()));
    auto stream_writer = check(arrow::ipc::MakeStreamWriter(
      ipc_ostream, record_batch->schema(),
      arrow::ipc::IpcWriteOptions{.memory_pool = tenzir::arrow_memory_pool(),
                                  .codec = {}}));
    auto status = stream_writer->WriteRecordBatch(*record_batch);
    if (not status.ok()) {
      TENZIR_ERROR("failed to write record batch: {}", status.ToString());
    }
    auto arrow_ipc_buffer = check(ipc_ostream->Finish());
    fbs_ipc_buffer = builder.CreateVector(arrow_ipc_buffer->data(),
                                          arrow_ipc_buffer->size());
  }
  // Create Arrow-encoded table slices. We need to set the import time to
  // something other than 0, as it cannot be modified otherwise. We then later
  // reset it to the clock's epoch.
  constexpr int64_t stub_ns_since_epoch = 1337;
  auto arrow_table_slice_buffer = fbs::table_slice::arrow::Createv2(
    builder, fbs_ipc_buffer, stub_ns_since_epoch);
  // Create and finish table slice.
  auto table_slice_buffer
    = fbs::CreateTableSlice(builder, fbs::table_slice::TableSlice::arrow_v2,
                            arrow_table_slice_buffer.Union());
  fbs::FinishTableSliceBuffer(builder, table_slice_buffer);
  // Create the table slice from the chunk.
  auto chunk = chunk::make(builder.Release());
  auto result = table_slice{std::move(chunk), table_slice::verify::no,
                            serialize == table_slice::serialize::yes
                              ? std::shared_ptr<arrow::RecordBatch>{}
                              : record_batch,
                            std::move(schema)};
  result.import_time(time{});
  return result;
}

/// @returns the type that `t` should be cast to, or `nullptr` if no casting
/// should take place.
auto target_type_for(const arrow::DataType& t)
  -> std::shared_ptr<arrow::DataType> {
  switch (t.id()) {
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::INT32:
      return arrow::int64();
    case arrow::Type::UINT8:
    case arrow::Type::UINT16:
    case arrow::Type::UINT32:
      return arrow::uint64();
    case arrow::Type::FLOAT:
      return arrow::float64();
    case arrow::Type::DURATION: {
      const auto& type = static_cast<const arrow::DurationType&>(t);
      if (type.unit() == arrow::TimeUnit::NANO) {
        return nullptr;
      }
      return arrow::duration(arrow::TimeUnit::NANO);
    }
    case arrow::Type::TIMESTAMP: {
      const auto& type = static_cast<const arrow::TimestampType&>(t);
      if (type.unit() == arrow::TimeUnit::NANO) {
        return nullptr;
      }
      return arrow::timestamp(arrow::TimeUnit::NANO);
    }
    default:
      return nullptr;
  }
}

auto try_convert_map_to_struct_array(std::shared_ptr<arrow::MapArray> map)
  -> std::shared_ptr<arrow::StructArray> {
  const auto& keys = map->keys();
  if (keys->type_id() != arrow::Type::STRING) {
    return nullptr;
  }
  const auto& values = map->items();
  if (values->type_id() != arrow::Type::STRING) {
    return nullptr;
  }
  auto keys_string = std::static_pointer_cast<arrow::StringArray>(keys);
  auto values_string = std::static_pointer_cast<arrow::StringArray>(values);
  auto builder = series_builder{};
  for (auto i = int64_t{0}; i < map->length(); ++i) {
    const auto start = map->value_offset(i);
    const auto end = start + map->value_length(i);
    if (map->IsNull(i)) {
      builder.null();
      continue;
    }
    auto rec = builder.record();
    for (auto j = start; j < end; ++j) {
      if (keys_string->IsNull(j)) {
        continue;
      }
      const auto key = keys_string->Value(j);
      if (values_string->IsNull(j)) {
        rec.field(key, caf::none);
        continue;
      }
      const auto value = values_string->Value(j);
      rec.field(key, value);
    }
  }
  auto s = builder.finish_assert_one_array();
  auto r = std::dynamic_pointer_cast<arrow::StructArray>(s.array);
  TENZIR_ASSERT(r);
  return r;
};

/// Tries to upgrade some column types to types supported by Tenzir, by
/// recursively checking their types and casting.
auto upgrade_arrays(const std::shared_ptr<arrow::Array>& array)
  -> std::shared_ptr<arrow::Array> {
  array->type();
  switch (array->type_id()) {
    case arrow::Type::MAP: {
      const auto map_array = std::static_pointer_cast<arrow::MapArray>(array);
      auto as_struct = try_convert_map_to_struct_array(map_array);
      if (as_struct) {
        return as_struct;
      }
      return array;
    }
    case arrow::Type::STRUCT: {
      auto struct_array = std::static_pointer_cast<arrow::StructArray>(array);
      auto new_arrays = std::vector<std::shared_ptr<arrow::Array>>{};
      auto new_fields = std::vector<std::shared_ptr<arrow::Field>>{};
      new_arrays.reserve(struct_array->num_fields());
      for (int i = 0; i < struct_array->num_fields(); ++i) {
        const auto& array = struct_array->field(i);
        const auto& field = struct_array->struct_type()->field(i);
        auto new_array = upgrade_arrays(struct_array->field(i));
        auto new_field = field;
        if (new_array != array) {
          new_field = new_field->WithType(new_array->type());
        }
        new_arrays.push_back(std::move(new_array));
        new_fields.push_back(std::move(new_field));
      }
      return make_struct_array(struct_array->length(),
                               struct_array->null_bitmap(),
                               std::move(new_fields), std::move(new_arrays));
    }
    case arrow::Type::LIST: {
      auto list_array = std::static_pointer_cast<arrow::ListArray>(array);
      auto values = upgrade_arrays(list_array->values());
      return check(arrow::ListArray::FromArrays(
        *list_array->offsets(), *values, arrow_memory_pool(),
        list_array->null_bitmap(), list_array->data()->null_count));
    }
    default: {
      if (auto target = target_type_for(*array->type())) {
        auto result = check(arrow::compute::Cast(array, target));
        return result.make_array();
      }
      return array;
    }
  }
  return array;
}

/// Tries to upgrade some column types to types supported by Tenzir, by
/// recursively checking their types and casting.
auto upgrade_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto new_columns = std::vector<std::shared_ptr<arrow::Array>>{};
  auto new_fields = std::vector<std::shared_ptr<arrow::Field>>{};
  new_columns.reserve(batch->num_columns());
  new_fields.reserve(batch->num_columns());
  for (auto i = 0; i < batch->num_columns(); ++i) {
    auto array = batch->column(i);
    const auto& field = batch->schema()->field(i);
    auto new_array = upgrade_arrays(array);
    auto new_field = field;
    if (array->type() != new_array->type()) {
      new_field = new_field->WithType(new_array->type());
    }
    new_columns.push_back(std::move(new_array));
    new_fields.push_back(std::move(new_field));
  }
  auto new_schema = std::make_shared<arrow::Schema>(
    new_fields, batch->schema()->endianness(), batch->schema()->metadata());
  return arrow::RecordBatch::Make(std::move(new_schema), batch->num_rows(),
                                  std::move(new_columns));
}

/// Verifies that an arrays type is supported by tenzir
/// @param arr the array to validate
/// @param[out] an error message on failure. If the array is invalid, `error`
///             will have a value.
/// @returns true if the array is supported, false otherwise. If false, `error`
///          will have a value.
auto verify_column(const arrow::Array& arr,
                   std::optional<table_slice::creation_error>& error) -> bool;

template <concrete_type T>
auto verify_column_impl(const type_to_arrow_array_t<T>* arr,
                        std::optional<table_slice::creation_error>&) -> bool {
  if (not arr) {
    return false;
  }
  return true;
}

template <>
auto verify_column_impl<tenzir::record_type>(
  const arrow::StructArray* arr,
  std::optional<table_slice::creation_error>& error) -> bool {
  if (not arr) {
    return false;
  }
  for (const auto& column : arr->fields()) {
    if (not verify_column(*column, error)) {
      return false;
    }
  }
  return true;
}

template <>
auto verify_column_impl<tenzir::list_type>(
  const arrow::ListArray* arr,
  std::optional<table_slice::creation_error>& error) -> bool {
  if (not arr) {
    return false;
  }
  return verify_column(*arr->values(), error);
}

template <>
auto verify_column_impl<tenzir::map_type>(
  const arrow::MapArray*, std::optional<table_slice::creation_error>&) -> bool {
  return false;
}

template <size_t I>
using type_type = typename caf::detail::tl_at<concrete_types, I>::type;

template <size_t I>
using array_type = type_to_arrow_array_t<type_type<I>>;

template <size_t... Is>
auto verify_column_fold(const arrow::Array& arr,
                        std::optional<table_slice::creation_error>& error,
                        std::index_sequence<Is...>) -> bool {
  return (verify_column_impl<type_type<Is>>(
            dynamic_cast<const array_type<Is>*>(&arr), error)
          or ...);
};

auto verify_column(const arrow::Array& arr,
                   std::optional<table_slice::creation_error>& error) -> bool {
  const auto valid = verify_column_fold(
    arr, error,
    std::make_index_sequence<caf::detail::tl_size_v<concrete_types>>{});
  if (valid) {
    return true;
  }
  // If it is not valid, we need to check if an error was set.
  // An error could already be set if we are further down in the recursion
  // stack. If there is no error, then our own array itself is the issue, and we
  // set an error.
  if (not error) {
    error.emplace(table_slice::creation_error{
      .message
      = fmt::format("unsupported arrow type `{}`", arr.type()->ToString())});
  }
  return false;
}

/// Verifies that an `arrow::RecordBatch`'s types are supported by Tenzir.
[[maybe_unused]] auto
verify_record_batch(const arrow::RecordBatch& record_batch)
  -> std::expected<void, table_slice::creation_error> {
  auto error = std::optional<table_slice::creation_error>{};
  for (const auto& column : record_batch.columns()) {
    if (not verify_column(*column, error)) {
      TENZIR_ASSERT(error);
      return std::unexpected(*error);
    }
  }
  return {};
}

/// Visits a FlatBuffers table slice to dispatch to its specific encoding.
/// @param visitor A callable object to dispatch to.
/// @param x The FlatBuffers root type for table slices.
/// @note The handler for invalid table slices takes no arguments. If none is
/// specified, visig aborts when the table slice encoding is invalid.
template <class Visitor>
auto visit(Visitor&& visitor, const fbs::TableSlice* x) noexcept(
  std::conjunction_v<
    // Check whether the handlers for all other table slice encodings are
    // noexcept-specified. When adding a new encoding, add it here as well.
    std::is_nothrow_invocable<Visitor>,
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::arrow::v2&>>) {
  if (not x) {
    return std::invoke(std::forward<Visitor>(visitor));
  }
  switch (x->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE:
      return std::invoke(std::forward<Visitor>(visitor));
    case fbs::table_slice::TableSlice::msgpack_v0:
    case fbs::table_slice::TableSlice::msgpack_v1:
    case fbs::table_slice::TableSlice::arrow_v0:
    case fbs::table_slice::TableSlice::arrow_v1:
      TENZIR_UNREACHABLE();
    case fbs::table_slice::TableSlice::arrow_v2:
      return std::invoke(std::forward<Visitor>(visitor),
                         *x->table_slice_as_arrow_v2());
  }
  // GCC-8 fails to recognize that this can never be reached, so we just call a
  // [[noreturn]] function.
  TENZIR_UNREACHABLE();
}

/// Get a pointer to the `tenzir.fbs.TableSlice` inside the chunk.
/// @param chunk The chunk to look at.
const fbs::TableSlice* as_flatbuffer(const chunk_ptr& chunk) noexcept {
  using flatbuffers::soffset_t;
  using flatbuffers::uoffset_t;
  if (not chunk || chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
    return nullptr;
  }
  return fbs::GetTableSlice(chunk->data());
}

/// Verifies that the chunk contains a valid `tenzir.fbs.TableSlice` FlatBuffers
/// table and returns the `chunk` itself, or returns `nullptr`.
/// @param chunk The chunk to verify.
/// @param verify Whether to verify the chunk.
/// @note This is a no-op if `verify == table_slice::verify::no`.
chunk_ptr
verified_or_none(chunk_ptr&& chunk, enum table_slice::verify verify) noexcept {
  if (verify == table_slice::verify::yes && chunk) {
    const auto* const data = reinterpret_cast<const uint8_t*>(chunk->data());
    auto verifier = flatbuffers::Verifier{data, chunk->size()};
    if (not verifier.template VerifyBuffer<fbs::TableSlice>()) {
      chunk = {};
    }
  }
  return std::move(chunk);
}

/// A helper utility for accessing the state of a table slice.
/// @param encoded The encoding-specific FlatBuffers table.
/// @param state The encoding-specific runtime state of the table slice.
template <class Slice, class State>
constexpr auto&
state([[maybe_unused]] Slice&& encoded, State&& state) noexcept {
  return std::forward<State>(state).arrow_v2;
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept = default;

table_slice::table_slice(chunk_ptr&& chunk, enum verify verify,
                         const std::shared_ptr<arrow::RecordBatch>& batch,
                         type schema) noexcept
  : chunk_{verified_or_none(std::move(chunk), verify)} {
  TENZIR_ASSERT(not chunk_ || chunk_->unique());
  if (chunk_) {
    auto f = detail::overload{
      []() noexcept {
        TENZIR_UNREACHABLE();
      },
      [&](const auto& encoded) noexcept {
        auto& state_ptr = state(encoded, state_);
        auto state = std::make_unique<std::decay_t<decltype(*state_ptr)>>(
          encoded, chunk_, batch, std::move(schema));
        state_ptr = state.get();
        const auto bytes = as_bytes(chunk_);
        // We create a second chunk with an intentionally decoupled reference
        // count here that we bind to the lifetime of the original chunk. This
        // avoids cyclic references between the table slice and its
        // encoding-specific state.
        chunk_
          = chunk::make(bytes, [state = std::move(state),
                                chunk = std::move(chunk_)]() mutable noexcept {
              // We manually call the destructors in proper order here, as the
              // state (and thus the contained chunk that actually owns the
              // memory we decoupled) must be destroyed last and the destruction
              // order for lambda captures is undefined.
              chunk = {};
              state = {};
            });
      },
    };
    visit(f, as_flatbuffer(chunk_));
  }
}

table_slice::table_slice(const fbs::FlatTableSlice& flat_slice,
                         const chunk_ptr& parent_chunk,
                         enum verify verify) noexcept
  : table_slice(parent_chunk->slice(as_bytes(*flat_slice.data())), verify) {
  // nop
}

table_slice::table_slice(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                         type schema, enum serialize serialize) {
  TENZIR_ASSERT_EXPENSIVE(verify_record_batch(*record_batch));
  auto builder = flatbuffers::FlatBufferBuilder{};
  *this
    = create_table_slice(record_batch, builder, std::move(schema), serialize);
}

auto table_slice::try_from(
  const std::shared_ptr<arrow::RecordBatch>& record_batch, type schema,
  enum serialize serialize) -> std::expected<table_slice, creation_error> {
  auto converted_batch = upgrade_record_batch(record_batch);
  auto valid = verify_record_batch(*converted_batch);
  if (not valid) {
    return std::unexpected(valid.error());
  }
  auto builder = flatbuffers::FlatBufferBuilder{};
  return create_table_slice(converted_batch, builder, std::move(schema),
                            serialize);
}

table_slice::table_slice(const table_slice& other) noexcept = default;

table_slice& table_slice::operator=(const table_slice& rhs) noexcept {
  if (this == &rhs) {
    return *this;
  }
  chunk_ = rhs.chunk_;
  offset_ = rhs.offset_;
  state_ = rhs.state_;
  return *this;
}

table_slice::table_slice(table_slice&& other) noexcept
  : chunk_{std::exchange(other.chunk_, {})},
    offset_{std::exchange(other.offset_, invalid_id)},
    state_{std::exchange(other.state_, {})} {
  // nop
}

table_slice& table_slice::operator=(table_slice&& rhs) noexcept {
  chunk_ = std::exchange(rhs.chunk_, {});
  offset_ = std::exchange(rhs.offset_, invalid_id);
  state_ = std::exchange(rhs.state_, {});
  return *this;
}

table_slice::~table_slice() noexcept = default;

table_slice table_slice::unshare() const noexcept {
  auto result = table_slice{chunk::copy(chunk_), verify::no};
  result.offset_ = offset_;
  return result;
}

// -- operators ----------------------------------------------------------------

// TODO: Dispatch to optimized implementations if the encodings are the same.
bool operator==(const table_slice& lhs, const table_slice& rhs) noexcept {
  if (not lhs.chunk_ && not rhs.chunk_) {
    return true;
  }
  constexpr auto check_metadata = true;
  return to_record_batch(lhs)->Equals(*to_record_batch(rhs), check_metadata);
}

bool operator!=(const table_slice& lhs, const table_slice& rhs) noexcept {
  return not(lhs == rhs);
}

// -- properties ---------------------------------------------------------------

const type& table_slice::schema() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      static const auto schema = type{};
      return &schema;
    },
    [&](const auto& encoded) noexcept {
      return &state(encoded, state_)->schema();
    },
  };
  return *visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::rows() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return size_type{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->rows();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::columns() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return size_type{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->columns();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

id table_slice::offset() const noexcept {
  return offset_;
}

void table_slice::offset(id offset) noexcept {
  offset_ = offset;
}

time table_slice::import_time() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return time{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->import_time();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

void table_slice::import_time(time import_time) noexcept {
  if (import_time == this->import_time()) {
    return;
  }
  modify_state([&](auto& state) {
    state.import_time(import_time);
  });
}

template <class F>
void table_slice::modify_state(F&& f) {
  // We work around the uniqueness requirement for the const_cast below by
  // creating a new table slice here that points to the same data as the current
  // table slice. This implies that the table slice is no longer in one
  // contiguous buffer.
  if (chunk_ && not chunk_->unique()) {
    *this = table_slice{to_record_batch(*this), schema()};
  }
  auto g = detail::overload{
    []() noexcept {
      TENZIR_UNREACHABLE();
    },
    [&](const auto& encoded) noexcept {
      auto& mutable_state
        = const_cast<std::add_lvalue_reference_t<std::remove_const_t<
          std::remove_reference_t<decltype(*state(encoded, state_))>>>>(
          *state(encoded, state_));
      f(mutable_state);
    },
  };
  visit(g, as_flatbuffer(chunk_));
}

bool table_slice::is_serialized() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return true;
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->is_serialized();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

// -- data access --------------------------------------------------------------

auto table_slice::values() const -> generator<view<record>> {
  auto path = tenzir::offset{};
  auto [type, array] = path.get(*this);
  const auto as_series = series{std::move(type), std::move(array)};
  for (auto&& value : as_series.values<record_type>()) {
    TENZIR_ASSERT_EXPENSIVE(value);
    co_yield std::move(*value);
  }
}

auto table_slice::values(const struct offset& path) const
  -> generator<view<data>> {
  auto [type, array] = path.get(*this);
  const auto as_series = series{std::move(type), std::move(array)};
  return as_series.values();
}

data_view table_slice::at(table_slice::size_type row,
                          table_slice::size_type column) const {
  TENZIR_ASSERT(row < rows());
  TENZIR_ASSERT(column < columns());
  auto f = detail::overload{
    [&]() noexcept -> data_view {
      TENZIR_ASSERT(false, "cannot access data of invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->at(row, column);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

data_view table_slice::at(table_slice::size_type row,
                          table_slice::size_type column, const type& t) const {
  TENZIR_ASSERT(row < rows());
  TENZIR_ASSERT(column < columns());
  auto f = detail::overload{
    [&]() noexcept -> data_view {
      TENZIR_ASSERT(false, "cannot access data of invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->at(row, column, t);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

std::shared_ptr<arrow::RecordBatch> to_record_batch(const table_slice& slice) {
  auto f = detail::overload{
    []() noexcept -> std::shared_ptr<arrow::RecordBatch> {
      TENZIR_ASSERT(false, "cannot access data of invalid table slice");
    },
    [&](const auto& encoded) noexcept -> std::shared_ptr<arrow::RecordBatch> {
      // The following does not work on all compilers, hence the ugly
      // decay+decltype workaround:
      //   if constexpr (state(encoding, slice.state_)->encoding
      //                 == table_slice_encoding::arrow) { ... }
      return state(encoded, slice.state_)->record_batch();
    },
  };
  return visit(f, as_flatbuffer(slice.chunk_));
}

// -- concepts -----------------------------------------------------------------

std::span<const std::byte> as_bytes(const table_slice& slice) noexcept {
  TENZIR_ASSERT(slice.is_serialized());
  return as_bytes(slice.chunk_);
}

auto size(const table_slice& slice) -> uint64_t {
  return slice.rows();
}

// -- operations ---------------------------------------------------------------

table_slice concatenate(std::vector<table_slice> slices) {
  slices.erase(std::remove_if(slices.begin(), slices.end(),
                              [](const auto& slice) {
                                return slice.rows() == 0;
                              }),
               slices.end());
  if (slices.empty()) {
    return {};
  }
  if (slices.size() == 1) {
    return std::move(slices[0]);
  }
  auto schema = slices[0].schema();
  TENZIR_ASSERT_EXPENSIVE(std::all_of(slices.begin(), slices.end(),
                                      [&](const auto& slice) {
                                        return slice.schema() == schema;
                                      }),
                          "concatenate requires slices to be homogeneous");
  auto builder
    = as<record_type>(schema).make_arrow_builder(arrow_memory_pool());
  auto arrow_schema = schema.to_arrow_schema();
  const auto resize_result
    = builder->Resize(detail::narrow_cast<int64_t>(rows(slices)));
  TENZIR_ASSERT(resize_result.ok(), resize_result.ToString().c_str());

  for (const auto& slice : slices) {
    auto batch = to_record_batch(slice);
    auto status = append_array(*builder, as<record_type>(schema),
                               *check(batch->ToStructArray()));
    TENZIR_ASSERT(status.ok());
  }
  const auto rows = builder->length();
  if (rows == 0) {
    return {};
  }
  const auto array = finish(*builder);
  auto batch
    = arrow::RecordBatch::Make(std::move(arrow_schema), rows, array->fields());
  auto result = table_slice{batch, schema};
  result.offset(slices[0].offset());
  result.import_time(slices[0].import_time());
  return result;
}

generator<table_slice>
select(const table_slice& slice, expression expr, const ids& hints) {
  if (slice.rows() == 0) {
    co_return;
  }
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  auto slice_ids = make_ids({{offset, offset + slice.rows()}});
  auto selection = slice_ids;
  if (not hints.empty()) {
    selection &= hints;
  }
  // Do no rows qualify?
  if (not any(selection)) {
    co_return;
  }
  // Evaluate the filter expression.
  if (not is<caf::none_t>(expr)) {
    // Tailor the expression to the type; this is required for using the
    // evaluate function, which expects field and type extractors to be resolved
    // already.
    auto tailored_expr = tailor(expr, slice.schema());
    if (not tailored_expr) {
      co_return;
    }
    selection = evaluate(*tailored_expr, slice, selection);
    // Do no rows qualify?
    if (not any(selection)) {
      co_return;
    }
  }
  // Do all rows qualify?
  if (rank(selection) == slice.rows()) {
    co_yield slice;
    co_return;
  }
  // Start slicing and dicing.
  auto batch = to_record_batch(slice);
  for (const auto [first, last] : select_runs(selection)) {
    co_yield subslice(slice, first - offset, last - offset);
  }
}

table_slice head(table_slice slice, size_t num_rows) {
  return subslice(
    slice, 0,
    std::min(slice.rows(),
             detail::narrow_cast<table_slice::size_type>(num_rows)));
}

table_slice tail(table_slice slice, size_t num_rows) {
  return subslice(
    slice,
    slice.rows()
      - std::min(slice.rows(),
                 detail::narrow_cast<table_slice::size_type>(num_rows)),
    slice.rows());
}

std::pair<table_slice, table_slice>
split(const table_slice& slice, size_t partition_point) {
  if (slice.rows() == 0) {
    return {{}, {}};
  }
  if (partition_point == 0) {
    return {{}, slice};
  }
  if (partition_point >= slice.rows()) {
    return {slice, {}};
  }
  return {
    head(slice, partition_point),
    tail(slice, slice.rows() - partition_point),
  };
}

auto split(std::vector<table_slice> events, uint64_t partition_point)
  -> std::pair<std::vector<table_slice>, std::vector<table_slice>> {
  auto it = events.begin();
  for (; it != events.end(); ++it) {
    if (partition_point == it->rows()) {
      return {
        {events.begin(), it + 1},
        {it + 1, events.end()},
      };
    }
    if (partition_point < it->rows()) {
      auto lhs = std::vector<table_slice>{};
      auto rhs = std::vector<table_slice>{};
      lhs.reserve(std::distance(events.begin(), it + 1));
      rhs.reserve(std::distance(it, events.end()));
      lhs.insert(lhs.end(), std::make_move_iterator(events.begin()),
                 std::make_move_iterator(it));
      auto [split_lhs, split_rhs] = split(*it, partition_point);
      lhs.push_back(std::move(split_lhs));
      rhs.push_back(std::move(split_rhs));
      rhs.insert(rhs.end(), std::make_move_iterator(it + 1),
                 std::make_move_iterator(events.end()));
      return {
        std::move(lhs),
        std::move(rhs),
      };
    }
    partition_point -= it->rows();
  }
  return {
    std::move(events),
    {},
  };
}

auto subslice(const table_slice& slice, size_t begin, size_t end)
  -> table_slice {
  TENZIR_ASSERT(begin <= end);
  TENZIR_ASSERT(end <= slice.rows());
  if (begin == 0 && end == slice.rows()) {
    return slice;
  }
  if (begin == end) {
    return {};
  }
  auto offset = slice.offset();
  auto batch = to_record_batch(slice);
  auto sub_slice = table_slice{
    batch->Slice(detail::narrow_cast<int64_t>(begin),
                 detail::narrow_cast<int64_t>(end - begin)),
    slice.schema(),
  };
  sub_slice.offset(offset + begin);
  sub_slice.import_time(slice.import_time());
  return sub_slice;
}

uint64_t rows(const std::vector<table_slice>& slices) {
  auto result = uint64_t{0};
  for (const auto& slice : slices) {
    result += slice.rows();
  }
  return result;
}

std::optional<table_slice>
filter(const table_slice& slice, expression expr, const ids& hints) {
  if (slice.rows() == 0) {
    return {};
  }
  auto selected = collect(select(slice, std::move(expr), hints));
  if (selected.empty()) {
    return {};
  }
  return concatenate(std::move(selected));
}

std::optional<table_slice>
filter(const table_slice& slice, const expression& expr) {
  return filter(slice, expr, ids{});
}

std::optional<table_slice> filter(const table_slice& slice, const ids& hints) {
  return filter(slice, expression{}, hints);
}

uint64_t count_matching(const table_slice& slice, const expression& expr,
                        const ids& hints) {
  if (slice.rows() == 0) {
    return 0;
  }
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  if (expr == expression{}) {
    auto result = uint64_t{};
    for (auto id : select(hints)) {
      if (id < offset) {
        continue;
      }
      if (id >= offset + slice.rows()) {
        break;
      }
      ++result;
    }
    return result;
  }
  // Tailor the expression to the type; this is required for using the
  // evaluate function, which expects field and type extractors to be resolved
  // already.
  auto tailored_expr = tailor(expr, slice.schema());
  if (not tailored_expr) {
    return 0;
  }
  return rank(evaluate(expr, slice, hints));
}

namespace {
constexpr static auto enumeration_to_string
  = [](series s) -> std::optional<series> {
  auto es = s.as<enumeration_type>();
  if (not es) {
    return std::nullopt;
  }
  auto new_type = tenzir::type{string_type{}};
  new_type.assign_metadata(s.type);
  auto builder = string_type::make_arrow_builder(arrow_memory_pool());
  for (const auto& value : es->values()) {
    if (not value) {
      check(builder->AppendNull());
      continue;
    }
    check(append_builder(string_type{}, *builder, es->type.field(*value)));
  }
  return series{
    std::move(new_type),
    check(builder->Finish()),
  };
};

template <typename T>
auto erase(std::pair<T, std::shared_ptr<type_to_arrow_array_t<T>>> v)
  -> std::pair<tenzir::type, std::shared_ptr<arrow::Array>> {
  return {tenzir::type{std::move(v.first)}, std::move(v.second)};
}
} // namespace

table_slice resolve_enumerations(table_slice slice) {
  return replace(std::move(slice), enumeration_to_string).second;
}

auto resolve_enumerations(series s) -> series {
  return replace(std::move(s), enumeration_to_string).second;
}

auto resolve_enumerations(
  tenzir::type type,
  const std::shared_ptr<type_to_arrow_array_t<tenzir::type>>& array)
  -> std::pair<tenzir::type, std::shared_ptr<arrow::Array>> {
  auto r
    = replace(series{std::move(type), array}, enumeration_to_string).second;
  return {
    std::move(r.type),
    std::move(r.array),
  };
}

auto resolve_enumerations(
  record_type schema, const std::shared_ptr<arrow::StructArray>& struct_array)
  -> std::pair<record_type, std::shared_ptr<arrow::StructArray>> {
  auto r = replace(basic_series<record_type>{std::move(schema), struct_array},
                   enumeration_to_string)
             .second;
  return {
    std::move(as<record_type>(r.type)),
    std::static_pointer_cast<arrow::StructArray>(r.array),
  };
}

auto resolve_enumerations(
  tenzir::list_type type,
  const std::shared_ptr<type_to_arrow_array_t<list_type>>& array)
  -> std::pair<tenzir::list_type,
               std::shared_ptr<type_to_arrow_array_t<list_type>>> {
  auto r = replace(basic_series<list_type>{std::move(type), array},
                   enumeration_to_string)
             .second;
  return {
    std::move(as<list_type>(r.type)),
    std::static_pointer_cast<type_to_arrow_array_t<list_type>>(r.array),
  };
}

auto resolve_enumerations(
  tenzir::enumeration_type type,
  const std::shared_ptr<enumeration_type::array_type>& array)
  -> std::pair<string_type, std::shared_ptr<arrow::StringArray>> {
  auto builder = arrow::StringBuilder(arrow_memory_pool());
  for (const auto& v : values3(*array)) {
    if (not v) {
      check(builder.AppendNull());
      continue;
    }
    check(builder.Append(type.field(*v)));
  }
  auto res = std::shared_ptr<arrow::StringArray>{};
  check(builder.Finish(&res));
  return {
    string_type{},
    std::move(res),
  };
}

auto resolve_meta_extractor(const table_slice& slice, const meta_extractor& ex)
  -> data {
  if (slice.rows() == 0) {
    return {};
  }
  switch (ex.kind) {
    case meta_extractor::schema: {
      return std::string{slice.schema().name()};
    }
    case meta_extractor::schema_id: {
      return slice.schema().make_fingerprint();
    }
    case meta_extractor::import_time: {
      const auto import_time = slice.import_time();
      if (import_time == time{}) {
        // The table slice API returns the epoch timestamp if no import
        // time was set, so we convert that to null.
        return {};
      }
      return import_time;
    }
    case meta_extractor::internal: {
      return slice.schema().attribute("internal").has_value();
    }
  }
  TENZIR_UNREACHABLE();
}

auto resolve_operand(const table_slice& slice, const operand& op)
  -> std::pair<type, std::shared_ptr<arrow::Array>> {
  if (slice.rows() == 0) {
    return {};
  }
  const auto batch = to_record_batch(slice);
  const auto& layout = as<record_type>(slice.schema());
  auto inferred_type = type{};
  auto array = std::shared_ptr<arrow::Array>{};
  // Helper function that binds a fixed value.
  auto bind_value = [&](const data& value) {
    auto tmp_inferred_type = type::infer(value);
    if (not tmp_inferred_type) {
      return;
    }
    inferred_type = *tmp_inferred_type;
    if (not inferred_type) {
      inferred_type = type{null_type{}};
      auto builder = null_type::make_arrow_builder(arrow_memory_pool());
      const auto append_result = builder->AppendNulls(batch->num_rows());
      TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
      array = finish(*builder);
      return;
    }
    match(inferred_type, [&]<concrete_type Type>(const Type& inferred_type) {
      auto builder = inferred_type.make_arrow_builder(arrow_memory_pool());
      for (int i = 0; i < batch->num_rows(); ++i) {
        const auto append_result = append_builder(
          inferred_type, *builder, make_view(as<type_to_data_t<Type>>(value)));
        TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
      }
      array = finish(*builder);
    });
  };
  // Helper function that binds an existing array.
  auto bind_array = [&](const offset& index) {
    inferred_type = layout.field(index).type;
    array = index.get(*batch);
  };
  match(
    op,
    [&](const data& value) {
      bind_value(value);
    },
    [&](const field_extractor& ex) {
      if (auto index = slice.schema().resolve_key_or_concept_once(ex.field)) {
        bind_array(*index);
        return;
      }
      bind_value({});
    },
    [&](const type_extractor& ex) {
      for (const auto& [field, index] : layout.leaves()) {
        if (field.type == ex.type or field.type.name() == ex.type.name()) {
          bind_array(index);
          return;
        }
      }
      bind_value({});
    },
    [&](const meta_extractor& ex) {
      bind_value(resolve_meta_extractor(slice, ex));
    },
    [&](const data_extractor& ex) {
      bind_array(layout.resolve_flat_index(ex.column));
    });
  return {
    std::move(inferred_type),
    std::move(array),
  };
}

namespace {

/// Combines offsets such that starting with the first array the list offsets
/// are replaced with the values of the next list offsets array, repeated until
/// all list offsets arrays have been combined into one. This allows for
/// flattening lists in Arrow's data model.
auto combine_offsets(
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets)
  -> std::shared_ptr<arrow::Array> {
  TENZIR_ASSERT(not list_offsets.empty());
  auto it = list_offsets.begin();
  auto result = *it++;
  auto builder = arrow::Int32Builder{};
  for (; it < list_offsets.end(); ++it) {
    for (const auto& index : static_cast<const arrow::Int32Array&>(*result)) {
      TENZIR_ASSERT(index);
      const auto& next = static_cast<const arrow::Int32Array&>(**it);
      TENZIR_ASSERT(not next.IsNull(*index));
      auto append_result = builder.Append(next.Value(*index));
      TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
    }
    result = check(builder.Finish());
  }
  return result;
}

auto make_flatten_transformation(
  std::string_view separator, const std::string& name_prefix,
  std::vector<std::shared_ptr<arrow::Array>> list_offsets)
  -> indexed_transformation::function_type;

auto flatten_record(
  std::string_view separator, std::string_view name_prefix,
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets,
  struct record_type::field field, const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type;

auto flatten_list(std::string_view separator, std::string_view name_prefix,
                  std::vector<std::shared_ptr<arrow::Array>> list_offsets,
                  struct record_type::field field,
                  const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type {
  const auto& lt = as<list_type>(field.type);
  auto list_array
    = std::static_pointer_cast<type_to_arrow_array_t<list_type>>(array);
  list_offsets.push_back(list_array->offsets());
  return match(
    lt.value_type(),
    [&]<concrete_type Type>(
      const Type&) -> indexed_transformation::result_type {
      auto result = indexed_transformation::result_type{};
      if (list_offsets.empty()) {
        result.push_back({
          {
            fmt::format("{}{}", name_prefix, field.name),
            field.type,
          },
          array,
        });
      } else {
        auto combined_offsets = combine_offsets(list_offsets);
        result.push_back({
          {
            fmt::format("{}{}", name_prefix, field.name),
            field.type,
          },
          check(arrow::ListArray::FromArrays(
            *combined_offsets, *list_array->values(), arrow_memory_pool())),
        });
      }
      return result;
    },
    [&](const list_type& lt) -> indexed_transformation::result_type {
      return flatten_list(separator, name_prefix, std::move(list_offsets),
                          {field.name, lt}, list_array->values());
    },
    [&](const record_type& rt) -> indexed_transformation::result_type {
      return flatten_record(separator, name_prefix, list_offsets,
                            {field.name, rt}, list_array->values());
    });
}

auto flatten_record(
  std::string_view separator, std::string_view name_prefix,
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets,
  struct record_type::field field, const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type {
  const auto& rt = as<record_type>(field.type);
  if (rt.num_fields() == 0) {
    return {};
  }
  auto struct_array
    = std::static_pointer_cast<type_to_arrow_array_t<record_type>>(array);
  const auto next_name_prefix
    = fmt::format("{}{}{}", name_prefix, field.name, separator);
  auto transformations = std::vector<indexed_transformation>{};
  for (size_t i = 0; i < rt.num_fields(); ++i) {
    transformations.push_back(
      {offset{i},
       make_flatten_transformation(separator, next_name_prefix, list_offsets)});
  }
  auto [output_type, output_struct_array]
    = transform_columns(field.type, struct_array, std::move(transformations));
  TENZIR_ASSERT(output_type);
  TENZIR_ASSERT(output_struct_array);
  auto result = indexed_transformation::result_type{};
  result.reserve(output_struct_array->num_fields());
  const auto& output_rt = as<record_type>(output_type);
  for (int i = 0; i < output_struct_array->num_fields(); ++i) {
    const auto field_view = output_rt.field(i);
    result.push_back({
      {std::string{field_view.name}, field_view.type},
      output_struct_array->field(i),
    });
  }
  return result;
}

auto make_flatten_transformation(
  std::string_view separator, const std::string& name_prefix,
  std::vector<std::shared_ptr<arrow::Array>> list_offsets)
  -> indexed_transformation::function_type {
  return
    [=](struct record_type::field field, std::shared_ptr<arrow::Array> array)
      -> indexed_transformation::result_type {
      return match(
        field.type,
        [&]<concrete_type Type>(
          const Type&) -> indexed_transformation::result_type {
          // Return unchanged, but use prefix, and wrap in a list if we need to.
          if (list_offsets.empty()) {
            return {
              {
                {
                  fmt::format("{}{}", name_prefix, field.name),
                  field.type,
                },
                array,
              },
            };
          }
          // FIXME: This transformation changes the length.
          auto combined_offsets = combine_offsets(list_offsets);
          return {
            {
              {
                fmt::format("{}{}", name_prefix, field.name),
                type{list_type{field.type}},
              },
              check(arrow::ListArray::FromArrays(*combined_offsets, *array,
                                                 arrow_memory_pool())),
            },
          };
        },
        [&](const list_type&) -> indexed_transformation::result_type {
          return flatten_list(separator, name_prefix, list_offsets,
                              std::move(field), array);
        },
        [&](const record_type&) -> indexed_transformation::result_type {
          return flatten_record(separator, name_prefix, list_offsets,
                                std::move(field), array);
        });
    };
}

auto make_rename_transformation(std::string new_name)
  -> indexed_transformation::function_type {
  return
    [new_name = std::move(new_name)](struct record_type::field field,
                                     std::shared_ptr<arrow::Array> array)
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      return {
        {
          {
            new_name,
            field.type,
          },
          array,
        },
      };
    };
}

} // namespace

auto flatten(type schema, const std::shared_ptr<arrow::StructArray>& array,
             std::string_view separator) -> flatten_array_result {
  // We cannot use arrow::StructArray::Flatten here because that does not
  // work recursively, see apache/arrow#20683. Hence, we roll our own version
  // here.
  auto renamed_fields = std::vector<std::string>{};
  auto transformations = std::vector<indexed_transformation>{};
  auto num_fields = as<record_type>(schema).num_fields();
  transformations.reserve(num_fields);
  for (size_t i = 0; i < num_fields; ++i) {
    transformations.push_back(
      {offset{i}, make_flatten_transformation(separator, "", {})});
  }
  const auto& [new_schema, transformed]
    = transform_columns(schema, array, std::move(transformations));
  // The slice may contain duplicate field name here, so we perform an
  // additional transformation to rename them in case we detect any.
  transformations.clear();
  const auto& layout = as<record_type>(new_schema);
  TENZIR_ASSERT_EXPENSIVE(layout.num_fields() == layout.num_leaves());
  for (const auto& leaf : layout.leaves()) {
    size_t num_occurences = 0;
    if (std::any_of(transformations.begin(), transformations.end(),
                    [&](const auto& t) {
                      return t.index == leaf.index;
                    })) {
      continue;
    }
    for (const auto& index : layout.resolve_key_suffix(leaf.field.name)) {
      // For historical reasons, resolve_key_suffix also suffix matches for dots
      // within a field name. That's pretty stupid, and it can lead to wrong
      // conflicts being detected here after flattening, so we check again if
      // the full name actually clashes.
      if (leaf.field.name != layout.field(index).name) {
        continue;
      }
      if (index <= leaf.index) {
        continue;
      }
      while (true) {
        ++num_occurences;
        if (num_occurences > 0) {
          auto new_name = fmt::format("{}_{}", leaf.field.name, num_occurences);
          if (layout.resolve_key(new_name).has_value()) {
            // The new field name also already exists, so we just continue
            // incrementing until we find a non-conflicting name.
            continue;
          }
          renamed_fields.push_back(
            fmt::format("{} -> {}", leaf.field.name, new_name));
          transformations.push_back(
            {index, make_rename_transformation(std::move(new_name))});
        }
        break;
      }
    }
  }
  const auto& [sch, arr]
    = transform_columns(new_schema, transformed, std::move(transformations));
  return {sch, arr, renamed_fields};
}

auto flatten(table_slice slice, std::string_view separator) -> flatten_result {
  if (slice.rows() == 0) {
    return {std::move(slice), {}};
  }
  if (as<record_type>(slice.schema()).num_fields() == 0) {
    return {std::move(slice), {}};
  }
  const auto& array = check(to_record_batch(slice)->ToStructArray());
  const auto& [schema, transformed, renamed]
    = flatten(slice.schema(), array, separator);
  if (not schema) {
    return {};
  }
  auto batch = arrow::RecordBatch::Make(
    schema.to_arrow_schema(), transformed->length(), transformed->fields());
  auto result = table_slice{batch, std::move(schema)};
  result.offset(slice.offset());
  result.import_time(slice.import_time());
  // Flattening cannot fail.
  TENZIR_ASSERT(result.rows() > 0);
  return {result, renamed};
}

namespace {

struct unflatten_entry;

/// Basically a mutable `arrow::StructArray`.
struct unflatten_record {
  unflatten_record(int64_t length, std::shared_ptr<arrow::Buffer> null_bitmap);

  int64_t length;
  std::shared_ptr<arrow::Buffer> null_bitmap;
  detail::stable_map<std::string_view, unflatten_entry> fields;
};

struct unflatten_entry
  : variant<std::shared_ptr<arrow::Array>, unflatten_record> {
  using variant::variant;
};

unflatten_record::unflatten_record(int64_t length,
                                   std::shared_ptr<arrow::Buffer> null_bitmap)
  : length{length}, null_bitmap{std::move(null_bitmap)} {
}

auto realize(unflatten_record&& record) -> std::shared_ptr<arrow::StructArray>;

auto realize(unflatten_entry&& entry) -> std::shared_ptr<arrow::Array> {
  return entry.match<std::shared_ptr<arrow::Array>>(
    [](std::shared_ptr<arrow::Array>& array) {
      TENZIR_ASSERT(array);
      return std::move(array);
    },
    [](unflatten_record& record) {
      return realize(std::move(record));
    });
}

auto realize(unflatten_record&& record) -> std::shared_ptr<arrow::StructArray> {
  auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
  auto arrays = std::vector<std::shared_ptr<arrow::Array>>{};
  for (auto& [name, entry] : record.fields) {
    auto array = realize(std::move(entry));
    fields.push_back(
      std::make_shared<arrow::Field>(std::string{name}, array->type()));
    arrays.push_back(std::move(array));
  }
  return make_struct_array(record.length, record.null_bitmap, std::move(fields),
                           arrays);
}

auto bitmap_or(const std::shared_ptr<arrow::Buffer>& x,
               const std::shared_ptr<arrow::Buffer>& y)
  -> std::shared_ptr<arrow::Buffer> {
  if (not x || not y) {
    return nullptr;
  }
  auto size = std::min(x->size(), y->size());
  auto z = check(arrow::AllocateBuffer(size));
  auto x_ptr = x->data();
  auto y_ptr = y->data();
  auto z_ptr = z->mutable_data();
  for (auto i = int64_t{0}; i < size; ++i) {
    z_ptr[i] = x_ptr[i] | y_ptr[i]; // NOLINT
  }
  return z;
}

void unflatten_into(unflatten_entry& entry, std::shared_ptr<arrow::Array> array,
                    std::string_view sep);

void unflatten_into(unflatten_entry& root, const arrow::StructArray& array,
                    std::string_view sep) {
  if (not std::holds_alternative<unflatten_record>(root)) {
    root.emplace<unflatten_record>(array.length(), array.null_bitmap());
  }
  auto names = array.struct_type()->fields()
               | std::views::transform(&arrow::Field::name);
  // We need to flatten the null bitmap here because it can happen that the
  // fields are saved to a record that is made non-null by another entry.
  auto fields = check(array.Flatten(tenzir::arrow_memory_pool()));
  for (auto [name, data] : detail::zip_equal(names, fields)) {
    auto segments
      = name | std::views::split(sep)
        | std::views::transform([](auto subrange) {
            return std::string_view{subrange.data(), subrange.size()};
          });
    auto current = &root;
    auto handle_segment = [&](std::string_view segment) {
      auto record = std::get_if<unflatten_record>(current);
      if (record) {
        record->null_bitmap
          = bitmap_or(record->null_bitmap, array.null_bitmap());
      } else {
        record = &current->emplace<unflatten_record>(array.length(),
                                                     array.null_bitmap());
      }
      current = &record->fields[segment];
    };
    // We have to work around the fact that `std::views::split` does not yield
    // anything if `name` is empty.
    if (segments.empty()) {
      handle_segment("");
    } else {
      for (auto segment : segments) {
        handle_segment(segment);
      }
    }
    unflatten_into(*current, data, sep);
  }
}

void unflatten_into(unflatten_entry& entry, std::shared_ptr<arrow::Array> array,
                    std::string_view sep) {
  if (auto record = try_as<arrow::StructArray>(*array)) {
    unflatten_into(entry, *record, sep);
  } else if (auto list = try_as<arrow::ListArray>(*array)) {
    entry = unflatten(*list, sep);
  } else {
    entry = std::move(array);
  }
}

} // namespace

auto unflatten(const arrow::StructArray& array, std::string_view sep)
  -> std::shared_ptr<arrow::StructArray> {
  // We unflatten records by recursively building up an `unflatten_record`,
  // which is basically a mutable `arrow::StructArray`.
  auto root = unflatten_entry{unflatten_record{array.length(), nullptr}};
  unflatten_into(root, array, sep);
  auto record = std::get_if<unflatten_record>(&root);
  TENZIR_ASSERT(record);
  return realize(std::move(*record));
}

auto unflatten(const arrow::ListArray& array, std::string_view sep)
  -> std::shared_ptr<arrow::ListArray> {
  // Unflattening a list simply means unflattening its values.
  auto values = unflatten(array.values(), sep);
  return check(arrow::ListArray::FromArrays(
    *array.offsets(), *values, arrow_memory_pool(), array.null_bitmap(),
    array.data()->null_count));
}

auto unflatten(std::shared_ptr<arrow::Array> array, std::string_view sep)
  -> std::shared_ptr<arrow::Array> {
  // We only unflatten records, but records can be contained in lists.
  if (auto record = try_as<arrow::StructArray>(*array)) {
    return unflatten(*record, sep);
  }
  if (auto list = try_as<arrow::ListArray>(*array)) {
    return unflatten(*list, sep);
  }
  return array;
}

auto unflatten(const table_slice& slice, std::string_view sep) -> table_slice {
  if (slice.rows() == 0) {
    return slice;
  }
  auto array = check(to_record_batch(slice)->ToStructArray());
  auto result = unflatten(array, sep);
  auto cast = std::dynamic_pointer_cast<arrow::StructArray>(std::move(result));
  TENZIR_ASSERT(cast);
  auto schema = type{slice.schema().name(), type::from_arrow(*cast->type())};
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                        cast->length(), cast->fields());
  auto out = table_slice{batch, std::move(schema)};
  out.import_time(slice.import_time());
  out.offset(slice.offset());
  return out;
}

auto table_slice::approx_bytes() const -> uint64_t {
  auto f = detail::overload{
    []() -> uint64_t {
      return 0;
    },
    [&](const auto& encoded) {
      return state(encoded, state_)->approx_bytes();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

auto columns_of(const table_slice& slice) -> generator<column_view> {
  const auto& schema = as<record_type>(slice.schema());
  const auto& batch = to_record_batch(slice);
  for (auto i = size_t{0}; i < schema.num_fields(); ++i) {
    auto field = schema.field(i);
    co_yield column_view{
      .name = field.name,
      .type = field.type,
      .array = *batch->column(i),
    };
  }
}

auto columns_of(const record_type& schema, const arrow::StructArray& array)
  -> generator<column_view> {
  for (auto [i, kt] : detail::enumerate(schema.fields())) {
    const auto& [k, t] = kt;
    auto offset = tenzir::offset{};
    offset.push_back(i);
    auto arr = offset.get(array);
    co_yield column_view{k, t, *arr};
  }
}

} // namespace tenzir
