//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_builder.hpp"

#include "vast/arrow_compat.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/config.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>

#include <simdjson.h>

namespace vast {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder::table_slice_builder(type layout,
                                         size_t initial_buffer_size)
  : layout_{std::move(layout)},
    schema_{layout_.to_arrow_schema()},
    arrow_builder_{
      this->layout().make_arrow_builder(arrow::default_memory_pool())},
    builder_{initial_buffer_size} {
  VAST_ASSERT(caf::holds_alternative<record_type>(layout_));
  VAST_ASSERT(!layout_.name().empty());
  VAST_ASSERT(schema_);
  for (auto&& leaf : caf::get<record_type>(layout_).leaves())
    leaves_.push_back(std::move(leaf));
  current_leaf_ = leaves_.end();
}

table_slice_builder::~table_slice_builder() noexcept {
  // nop
}

// -- properties ---------------------------------------------------------------

size_t table_slice_builder::columns() const noexcept {
  auto result = schema_->num_fields();
  VAST_ASSERT(result >= 0);
  return detail::narrow_cast<size_t>(result);
}

namespace {

/// Create a table slice from a record batch.
/// @param record_batch The record batch to encode.
/// @param builder The flatbuffers builder to use.
/// @param serialize Embed the IPC format in the FlatBuffers table.
table_slice
create_table_slice(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                   flatbuffers::FlatBufferBuilder& builder, type schema,
                   table_slice::serialize serialize) {
  VAST_ASSERT(record_batch);
#if VAST_ENABLE_ASSERTIONS
  // NOTE: There's also a ValidateFull function, but that always errors when
  // using nested struct arrays. Last tested with Arrow 7.0.0. -- DL.
  auto validate_status = record_batch->Validate();
  VAST_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
#endif // VAST_ENABLE_ASSERTIONS
  auto fbs_ipc_buffer = flatbuffers::Offset<flatbuffers::Vector<uint8_t>>{};
  if (serialize == table_slice::serialize::yes) {
    auto ipc_ostream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto opts = arrow::ipc::IpcWriteOptions::Defaults();
    opts.codec
      = arrow::util::Codec::Create(
          arrow::Compression::ZSTD,
          arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
            .ValueOrDie())
          .ValueOrDie();
    auto stream_writer
      = arrow::ipc::MakeStreamWriter(ipc_ostream, record_batch->schema(), opts)
          .ValueOrDie();
    auto status = stream_writer->WriteRecordBatch(*record_batch);
    if (!status.ok())
      VAST_ERROR("failed to write record batch: {}", status.ToString());
    auto arrow_ipc_buffer = ipc_ostream->Finish().ValueOrDie();
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
  auto chunk = fbs::release(builder);
  auto result = table_slice{std::move(chunk), table_slice::verify::no,
                            serialize == table_slice::serialize::yes
                              ? std::shared_ptr<arrow::RecordBatch>{}
                              : record_batch,
                            std::move(schema)};
  result.import_time(time{});
  return result;
}

[[maybe_unused]] bool
verify_record_batch(const arrow::RecordBatch& record_batch) {
  auto check_col
    = [](auto&& check_col, const arrow::Array& column) noexcept -> void {
    auto f = detail::overload{
      [&](const arrow::StructArray& sa) noexcept {
        for (const auto& column : sa.fields())
          check_col(check_col, *column);
      },
      [&](const arrow::ListArray& la) noexcept {
        check_col(check_col, *la.values());
      },
      [&](const arrow::MapArray& ma) noexcept {
        check_col(check_col, *ma.keys());
        check_col(check_col, *ma.items());
      },
      [](const arrow::Array&) noexcept {},
    };
    caf::visit(f, column);
  };
  for (const auto& column : record_batch.columns())
    check_col(check_col, *column);
  return true;
}

} // namespace

table_slice table_slice_builder::finish() {
  // Sanity check: If this triggers, the calls to add() did not match the number
  // of fields in the layout.
  VAST_ASSERT(current_leaf_ == leaves_.end());
  // Pack record batch.
  auto combined_array = arrow_builder_->Finish().ValueOrDie();
  auto record_batch = arrow::RecordBatch::Make(
    schema_, detail::narrow_cast<int64_t>(num_rows_),
    caf::get<type_to_arrow_array_t<record_type>>(*combined_array).fields());
  // Reset the builder state.
  num_rows_ = {};
  return create_table_slice(record_batch, this->builder_, layout(),
                            table_slice::serialize::yes);
}

table_slice table_slice_builder::create(
  const std::shared_ptr<arrow::RecordBatch>& record_batch, type schema,
  table_slice::serialize serialize, size_t initial_buffer_size) {
  VAST_ASSERT(verify_record_batch(*record_batch));
  auto builder = flatbuffers::FlatBufferBuilder{initial_buffer_size};
  return create_table_slice(record_batch, builder, std::move(schema),
                            serialize);
}

size_t table_slice_builder::rows() const noexcept {
  return num_rows_;
}

void table_slice_builder::reserve([[maybe_unused]] size_t num_rows) {
  // nop
}

const type& table_slice_builder::layout() const noexcept {
  return layout_;
}

// -- implementation details ---------------------------------------------------

bool table_slice_builder::recursive_add(const data& x, const type& t) {
  auto f = detail::overload{
    [&](const record& xs, const record_type& rt) {
      if (xs.size() != rt.num_fields())
        return false;
      for (const auto& field : rt.fields()) {
        const auto it = xs.find(field.name);
        if (it == xs.end())
          return false;
        if (!recursive_add(it->second, field.type))
          return false;
      }
      return true;
    },
    [&](const list& xs, const record_type& rt) {
      size_t col = 0;
      for (const auto& field : rt.fields()) {
        VAST_ASSERT(col < xs.size());
        if (!recursive_add(xs[col], field.type))
          return false;
        ++col;
      }
      return col == xs.size();
    },
    [&](const list& xs, const list_type& lt) {
      // table_slice_builder::recursive_add's purpose is to add a whole row at
      // once that is represented as a `vast::data`. The internal data
      // representations for both Arrow and MsgPack encododings are flattened,
      // except for record types that exist in lists, which we need to unflatten
      // from lists of values into records here.
      // A way better solution would be to rethink the table_slice_builder API
      // from the ground up once we stop flattening data, and removing the need
      // for this recursive_add function.
      // TODO: In the meantime we should try to replace all use cases for this
      // function with inline add calls, traversing views on the parsed data
      // rather than converting events into lists first.
      auto unwrap_nested = [](auto&& self, data x, const type& t) -> data {
        // (1) Try to unwrap list into lists by applying unwrap_nested
        // recursively.
        if (const auto* lt = caf::get_if<list_type>(&t)) {
          auto* l = caf::get_if<list>(&x);
          VAST_ASSERT(l);
          auto result = list{};
          result.reserve(l->size());
          for (size_t i = 0; i < l->size(); ++i)
            result.emplace_back(self(self, (*l)[i], lt->value_type()));
          return result;
        }
        // (2) Try to unwrap list into records.
        if (const auto* rt = caf::get_if<record_type>(&t)) {
          // This special case handles the situation where we have a record
          // inside a list or a map, for which we do not add nil values for
          // missing fields.
          if (caf::holds_alternative<caf::none_t>(x))
            return caf::none;
          auto* l = caf::get_if<list>(&x);
          VAST_ASSERT(l);
          VAST_ASSERT(l->size() == rt->num_fields());
          auto result = record{};
          result.reserve(l->size());
          for (size_t i = 0; i < l->size(); ++i) {
            const auto field = rt->field(i);
            result.emplace(std::string{field.name},
                           self(self, (*l)[i], field.type));
          }
          return result;
        }
        // (3) We're done unwrapping.
        return x;
      };
      return add(unwrap_nested(unwrap_nested, xs, type{lt}));
    },
    [&](const auto&, const auto&) {
      return add(make_view(x));
    },
  };
  return caf::visit(f, x, t);
}

bool table_slice_builder::add(data_view x) {
  auto* nested_builder
    = &caf::get<type_to_arrow_builder_t<record_type>>(*arrow_builder_);
  if (num_rows_ == 0 || current_leaf_ == leaves_.end()) {
    current_leaf_ = leaves_.begin();
    if (auto status = nested_builder->Append(); !status.ok()) {
      VAST_ERROR("failed to add row to builder with schema {}: {}", layout(),
                 status.ToString());
      return false;
    }
    ++num_rows_;
  }
  auto&& [field, index] = std::move(*current_leaf_);
  for (size_t i = 0; i < index.size() - 1; ++i) {
    nested_builder = &caf::get<type_to_arrow_builder_t<record_type>>(
      *nested_builder->field_builder(detail::narrow_cast<int>(index[i])));
    if (index.back() == 0) {
      if (auto status = nested_builder->Append(); !status.ok()) {
        VAST_ERROR("failed to add nested record to builder with schema {}: "
                   "{}",
                   layout(), status.ToString());
        return false;
      }
    }
  }
  if (auto status = append_builder(
        field.type,
        *nested_builder->field_builder(detail::narrow_cast<int>(index.back())),
        x);
      !status.ok()) {
    VAST_ERROR("failed to add {} to builder for field {}: {}", x, field,
               status.ToString());
    return false;
  }
  ++current_leaf_;
  return true;
}

// -- column builder helpers --------------------------------------------------

arrow::Status
append_builder(const bool_type&, type_to_arrow_builder_t<bool_type>& builder,
               const view<type_to_data_t<bool_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const integer_type&,
               type_to_arrow_builder_t<integer_type>& builder,
               const view<type_to_data_t<integer_type>>& view) noexcept {
  return builder.Append(view.value);
}

arrow::Status
append_builder(const count_type&, type_to_arrow_builder_t<count_type>& builder,
               const view<type_to_data_t<count_type>>& view) noexcept {
  return builder.Append(view);
}

arrow::Status
append_builder(const real_type&, type_to_arrow_builder_t<real_type>& builder,
               const view<type_to_data_t<real_type>>& view) noexcept {
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
  return builder.Append(arrow_compat::string_view{view.data(), view.size()});
}

arrow::Status
append_builder(const pattern_type&,
               type_to_arrow_builder_t<pattern_type>& builder,
               const view<type_to_data_t<pattern_type>>& view) noexcept {
  const auto str = view.string();
  return builder.Append(arrow_compat::string_view{str.data(), str.size()});
}

arrow::Status
append_builder(const address_type&,
               type_to_arrow_builder_t<address_type>& builder,
               const view<type_to_data_t<address_type>>& view) noexcept {
  const auto bytes = as_bytes(view);
  VAST_ASSERT(bytes.size() == 16);
  return builder.Append(arrow_compat::string_view{
    reinterpret_cast<const char*>(bytes.data()), bytes.size()});
}

arrow::Status
append_builder(const subnet_type&,
               type_to_arrow_builder_t<subnet_type>& builder,
               const view<type_to_data_t<subnet_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok())
    return status;
  if (auto status = append_builder(address_type{}, builder.address_builder(),
                                   view.network());
      !status.ok())
    return status;
  return builder.length_builder().Append(view.length());
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
  if (auto status = builder.Append(); !status.ok())
    return status;
  auto append_values = [&](const concrete_type auto& value_type) noexcept {
    auto& value_builder = *builder.value_builder();
    for (const auto& value_view : view)
      if (auto status = append_builder(value_type, value_builder, value_view);
          !status.ok())
        return status;
    return arrow::Status::OK();
  };
  return caf::visit(append_values, hint.value_type());
}

arrow::Status
append_builder(const map_type& hint, type_to_arrow_builder_t<map_type>& builder,
               const view<type_to_data_t<map_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok())
    return status;
  auto append_values = [&](const concrete_type auto& key_type,
                           const concrete_type auto& item_type) noexcept {
    auto& key_builder = *builder.key_builder();
    auto& item_builder = *builder.item_builder();
    for (const auto& [key_view, item_view] : view) {
      if (auto status = append_builder(key_type, key_builder, key_view);
          !status.ok())
        return status;
      if (auto status = append_builder(item_type, item_builder, item_view);
          !status.ok())
        return status;
    }
    return arrow::Status::OK();
  };
  return caf::visit(append_values, hint.key_type(), hint.value_type());
}

arrow::Status
append_builder(const record_type& hint,
               type_to_arrow_builder_t<record_type>& builder,
               const view<type_to_data_t<record_type>>& view) noexcept {
  if (auto status = builder.Append(); !status.ok())
    return status;
  for (int index = 0; const auto& [_, field_type] : hint.fields()) {
    if (auto status = append_builder(field_type, *builder.field_builder(index),
                                     view->at(index).second);
        !status.ok())
      return status;
    ++index;
  }
  return arrow::Status::OK();
}

arrow::Status append_builder(const type& hint,
                             std::same_as<arrow::ArrayBuilder> auto& builder,
                             const view<type_to_data_t<type>>& value) noexcept {
  if (caf::holds_alternative<caf::none_t>(value))
    return builder.AppendNull();
  auto f = [&]<concrete_type Type>(const Type& hint) {
    return append_builder(hint,
                          caf::get<type_to_arrow_builder_t<Type>>(builder),
                          caf::get<view<type_to_data_t<Type>>>(value));
  };
  return caf::visit(f, hint);
}

} // namespace vast
