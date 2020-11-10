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

#include "vast/table_slice.hpp"

#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/factory.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/error.hpp>
#include <caf/execution_unit.hpp>
#include <caf/sec.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include <unordered_map>

#if VAST_HAVE_ARROW
#  include "vast/arrow_table_slice.hpp"
#endif // VAST_HAVE_ARROW

namespace vast {

// -- utility functions --------------------------------------------------------

namespace {

/// Visits a FlatBuffers table slice to dispatch to its specific encoding.
/// @param visitor A callable object to dispatch to.
/// @param x The FlatBuffers root type for table slices.
/// @note The handler for invalid table slices takes no arguments. If none is
/// specified, visig aborts when the table slice encoding is invalid.
template <class Visitor>
auto visit(Visitor&& visitor, const fbs::TableSlice* x) noexcept(
  std::conjunction_v<
    // Check whether the handler for invalid encodings is noexcept-specified,
    // if and only if it exists.
    std::disjunction<std::negation<std::is_invocable<Visitor>>,
                     std::is_nothrow_invocable<Visitor>>,
    // Check whether the handlers for all other table slice encodings are
    // noexcept-specified. When adding a new encoding, add it here as well.
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::legacy::v0*>,
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::msgpack::v0*>>) {
  if (!x) {
    if constexpr (std::is_invocable_v<Visitor>)
      return std::invoke(std::forward<Visitor>(visitor));
    else
      die("visitor cannot handle invalid table slices");
  }
  switch (x->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE:
      if constexpr (std::is_invocable_v<Visitor>)
        return std::invoke(std::forward<Visitor>(visitor));
      else
        die("visitor cannot handle table slices with an invalid encoding");
    case fbs::table_slice::TableSlice::legacy_v0:
      return std::invoke(std::forward<Visitor>(visitor),
                         x->table_slice_as_legacy_v0());
    case fbs::table_slice::TableSlice::msgpack_v0:
      return std::invoke(std::forward<Visitor>(visitor),
                         x->table_slice_as_msgpack_v0());
  }
  // GCC-8 fails to recognize that this can never be reached, so we just call a
  // [[noreturn]] function.
  die("unhandled table slice encoding");
}

/// Get a pointer to the `vast.fbs.TableSlice` inside the chunk.
/// @param chunk The chunk to look at.
const fbs::TableSlice* as_flatbuffer(const chunk_ptr& chunk) noexcept {
  if (!chunk)
    return nullptr;
  return fbs::GetTableSlice(chunk->data());
}

/// Verifies that the chunk contains a valid `vast.fbs.TableSlice` FlatBuffers
/// table and returns the `chunk` itself, or returns `nullptr`.
/// @param chunk The chunk to verify.
/// @param verify Whether to verify the chunk.
/// @note This is a no-op if `verify == table_slice::verify::no`.
chunk_ptr
verified_or_none(chunk_ptr&& chunk, enum table_slice::verify verify) noexcept {
  if (verify == table_slice::verify::yes && chunk) {
    const auto data = reinterpret_cast<const uint8_t*>(chunk->data());
    auto verifier = flatbuffers::Verifier{data, chunk->size()};
    if (!verifier.template VerifyBuffer<fbs::TableSlice>())
      chunk = {};
  }
  return std::move(chunk);
}

/// A helper utility for rebuilding an existing table slice with a new builder.
/// @param slice The table slice to rebuild.
/// @param args... Arguments to pass to the table slice builder factory.
template <class... Args>
table_slice rebuild_slice(const table_slice& slice, Args&&... args) {
  auto builder
    = factory<table_slice_builder>::make(std::forward<Args>(args)...);
  if (!builder)
    return {};
  for (table_slice::size_type row = 0; row < slice.rows(); ++row)
    for (table_slice::size_type column = 0; column < slice.columns(); ++column)
      if (!builder->add(slice.at(row, column)))
        return {};
  return builder->finish();
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept {
  // nop
}

table_slice::table_slice(chunk_ptr&& chunk, enum verify verify) noexcept
  : chunk_{verified_or_none(std::move(chunk), verify)} {
  if (chunk_ && chunk_->unique()) {
    ++num_instances_;
    chunk_->add_deletion_step([]() noexcept { --num_instances_; });
  }
  visit(detail::overload{
          // Ignore everything...
          [](auto&&...) noexcept {},
          // ... except for legacy encoded chunks.
          [&](const fbs::table_slice::legacy::v0* slice) noexcept {
            if (auto error = unpack(*slice, legacy_))
              die("failed to unpack already verified legacy table slice: "
                  + render(error));
          },
        },
        as_flatbuffer(chunk_));
}

table_slice::table_slice(const fbs::FlatTableSlice& flat_slice,
                         const chunk_ptr& parent_chunk,
                         enum verify verify) noexcept {
  const auto flat_slice_begin
    = reinterpret_cast<const char*>(flat_slice.data()->data());
  const auto flat_slice_size = flat_slice.data()->size();
  VAST_ASSERT(flat_slice_begin >= parent_chunk->begin());
  VAST_ASSERT(flat_slice_begin + flat_slice_size <= parent_chunk->end());
  auto chunk = parent_chunk->slice(flat_slice_begin - parent_chunk->begin(),
                                   flat_slice_size);
  // Delegate the sliced chunk to the constructor.
  *this = table_slice{std::move(chunk), verify};
}

// FIXME: Remove this when removing legacy table slices.
table_slice::table_slice(legacy_table_slice_ptr&& slice) noexcept {
  if (slice) {
    flatbuffers::FlatBufferBuilder builder{};
    // Pack layout.
    std::vector<char> serialized_layout;
    caf::binary_serializer layout_sink{nullptr, serialized_layout};
    if (auto error = layout_sink(slice->layout()))
      die("failed to pack legacy table slice: " + render(error));
    auto layout_buffer = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(serialized_layout.data()),
      serialized_layout.size());
    // Pack data.
    std::vector<char> serialized_data;
    caf::binary_serializer data_sink{nullptr, serialized_data};
    if (auto error = data_sink(slice))
      die("failed to pack legacy table slice: " + render(error));
    auto data_buffer = builder.CreateVector(
      reinterpret_cast<const uint8_t*>(serialized_data.data()),
      serialized_data.size());
    // Transform encoding.
    auto transform = [](caf::atom_value slice)
      -> caf::expected<fbs::table_slice::legacy::Encoding> {
      if (slice == caf::atom("arrow"))
        return fbs::table_slice::legacy::Encoding::Arrow;
      if (slice == caf::atom("msgpack"))
        return fbs::table_slice::legacy::Encoding::MessagePack;
      return make_error(ec::unspecified, "unsupported table slice type", slice);
    };
    auto encoding = transform(slice->implementation_id());
    if (!encoding)
      die("failed to pack legacy table slice: " + render(encoding.error()));
    // Pack legacy table slice.
    auto legacy_table_slice_buffer
      = fbs::table_slice::legacy::Createv0(builder, slice->offset(),
                                           slice->rows(), layout_buffer,
                                           *encoding, data_buffer);
    // Pack table slice.
    auto table_slice_buffer
      = fbs::CreateTableSlice(builder, fbs::table_slice::TableSlice::legacy_v0,
                              legacy_table_slice_buffer.Union());
    fbs::FinishTableSliceBuffer(builder, table_slice_buffer);
    auto chunk = fbs::release(builder);
    // Delegate the newly created chunk to the constructor.
    *this = table_slice{std::move(chunk), verify::no};
    VAST_ASSERT(*legacy_ == *slice);
  }
}

table_slice::table_slice(const table_slice& other) noexcept
  : chunk_{other.chunk_}, legacy_{other.legacy_} {
  // nop
}

table_slice::table_slice(const table_slice& other, enum encoding encoding,
                         enum verify verify) noexcept {
  // FIXME: When switching to versioned encodings, perform re-encoding when the
  // encoding version is outdated, too.
  if (encoding == other.encoding()) {
    chunk_ = other.chunk_;
    legacy_ = other.legacy_;
  } else {
    switch (encoding) {
      case encoding::none:
        // Do nothing, we have an invalid table slice here.
        break;
      case encoding::arrow:
        *this = rebuild_slice(other, caf::atom("arrow"), other.layout());
        break;
      case encoding::msgpack:
        *this = rebuild_slice(other, caf::atom("msgpack"), other.layout());
        break;
    }
    // If requested, verify the chunk after performing re-encoding.
    chunk_ = verified_or_none(std::move(chunk_), verify);
  }
}

table_slice& table_slice::operator=(const table_slice& rhs) noexcept {
  chunk_ = rhs.chunk_;
  legacy_ = rhs.legacy_;
  return *this;
}

table_slice::table_slice(table_slice&& other) noexcept
  : chunk_{std::exchange(other.chunk_, {})},
    legacy_{std::exchange(other.legacy_, {})} {
  // nop
}

table_slice::table_slice(table_slice&& other, enum encoding encoding,
                         enum verify verify) noexcept {
  if (encoding == other.encoding()) {
    // If the encoding matches, we can just move the data.
    chunk_ = std::exchange(other.chunk_, {});
    legacy_ = std::exchange(other.legacy_, {});
  } else {
    // Changing the encoding requires a copy, so we just delegate to the
    // copy-constructor with re-encoding.
    const auto& copy = std::exchange(other, {});
    *this = table_slice{copy, encoding, verify};
  }
}

table_slice& table_slice::operator=(table_slice&& rhs) noexcept {
  chunk_ = std::exchange(rhs.chunk_, {});
  legacy_ = std::exchange(rhs.legacy_, {});
  return *this;
}

table_slice::~table_slice() noexcept {
  // nop
}

// -- operators ----------------------------------------------------------------

bool operator==(const table_slice& lhs, const table_slice& rhs) noexcept {
  // Check whether the slices point to the same chunk of data.
  if (lhs.chunk_ == rhs.chunk_)
    return true;
  // Check whether the slices have different sizes or layouts.
  if (lhs.rows() != rhs.rows() || lhs.columns() != rhs.columns()
      || lhs.layout() != rhs.layout())
    return false;
  // Check whether the slices contain different data.
  for (size_t row = 0; row < lhs.rows(); ++row)
    for (size_t col = 0; col < lhs.columns(); ++col)
      if (lhs.at(row, col) != rhs.at(row, col))
        return false;
  return true;
}

bool operator!=(const table_slice& lhs, const table_slice& rhs) noexcept {
  return !(lhs == rhs);
}

// -- properties ---------------------------------------------------------------

enum table_slice::encoding table_slice::encoding() const noexcept {
  auto f = detail::overload{
    []() noexcept { return encoding::none; },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      if (legacy_->implementation_id() == caf::atom("arrow"))
        return encoding::arrow;
      if (legacy_->implementation_id() == caf::atom("msgpack"))
        return encoding::msgpack;
      return encoding::none;
    },
    [](const fbs::table_slice::msgpack::v0*) noexcept {
      return encoding::msgpack;
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

record_type table_slice::layout() const noexcept {
  auto f = detail::overload{
    []() noexcept { return record_type{}; },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      return legacy_->layout();
    },
    [](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.layout();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::rows() const noexcept {
  auto f = detail::overload{
    []() noexcept { return size_type{}; },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      return legacy_->rows();
    },
    [](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.rows();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::columns() const noexcept {
  auto f = detail::overload{
    []() noexcept { return size_type{}; },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      return legacy_->columns();
    },
    [](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.columns();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

id table_slice::offset() const noexcept {
  auto f = detail::overload{
    []() noexcept { return invalid_id; },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      return legacy_->offset();
    },
    [](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.offset();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

void table_slice::offset(id offset) noexcept {
  VAST_ASSERT(encoding() != encoding::none);
  auto f = detail::overload{
    []() noexcept {
      // nop
    },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      legacy_.unshared().offset(offset);
      *this = table_slice{std::move(legacy_)};
    },
    [&](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.offset(offset);
    },
  };
  visit(f, as_flatbuffer(chunk_));
}

int table_slice::instances() noexcept {
  return num_instances_;
}

// -- data access --------------------------------------------------------------

/// Appends all values in column `column` to `index`.
/// @param `column` The index of the column to append.
/// @param `index` the value index to append to.
void table_slice::append_column_to_index(table_slice::size_type column,
                                         value_index& index) const {
  auto f = detail::overload{
    []() noexcept {
      // nop
    },
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      legacy_->append_column_to_index(column, index);
    },
    [&](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.append_column_to_index(offset(),
                                                                column, index);
    },
  };
  visit(f, as_flatbuffer(chunk_));
}

/// Retrieves data by specifying 2D-coordinates via row and column.
/// @param row The row offset.
/// @param column The column offset.
/// @pre `row < rows() && column < columns()`
data_view table_slice::at(table_slice::size_type row,
                          table_slice::size_type column) const {
  VAST_ASSERT(row < rows());
  VAST_ASSERT(column < columns());
  auto f = detail::overload{
    [&](const fbs::table_slice::legacy::v0*) noexcept {
      return legacy_->at(row, column);
    },
    [&](const fbs::table_slice::msgpack::v0* slice) noexcept {
      return msgpack_table_slice{*slice}.at(row, column);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

#if VAST_HAVE_ARROW

std::shared_ptr<arrow::RecordBatch> as_record_batch(const table_slice& slice) {
  auto f = detail::overload{
    [](auto&&...) noexcept -> std::shared_ptr<arrow::RecordBatch> {
      return nullptr;
    },
    [&](const fbs::table_slice::legacy::v0*) noexcept
    -> std::shared_ptr<arrow::RecordBatch> {
      if (slice.legacy_->implementation_id() == caf::atom("arrow"))
        return static_cast<const arrow_table_slice&>(*slice.legacy_).batch();
      return nullptr;
    },
  };
  return visit(f, as_flatbuffer(slice.chunk_));
}

#endif // VAST_HAVE_ARROW

// -- concepts -----------------------------------------------------------------

span<const byte> as_bytes(const table_slice& slice) noexcept {
  return as_bytes(slice.chunk_);
}

// -- legacy_table_slice -------------------------------------------------------

namespace {

using size_type = legacy_table_slice::size_type;

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

legacy_table_slice::legacy_table_slice() noexcept {
  ++instance_count_;
}

legacy_table_slice::legacy_table_slice(const legacy_table_slice& other) noexcept
  : ref_counted(other), header_{other.header_} {
  ++instance_count_;
}

legacy_table_slice&
legacy_table_slice::operator=(const legacy_table_slice& rhs) noexcept {
  header_ = rhs.header_;
  ++instance_count_;
  return *this;
}

legacy_table_slice::legacy_table_slice(legacy_table_slice&& other) noexcept
  : header_{std::exchange(other.header_, {})} {
  // nop
}

legacy_table_slice&
legacy_table_slice::operator=(legacy_table_slice&& rhs) noexcept {
  header_ = std::exchange(rhs.header_, {});
  return *this;
}

legacy_table_slice::legacy_table_slice(table_slice_header header) noexcept
  : header_{std::move(header)} {
  ++instance_count_;
}

legacy_table_slice::~legacy_table_slice() noexcept {
  --instance_count_;
}

// -- persistence --------------------------------------------------------------

caf::error legacy_table_slice::load(chunk_ptr chunk) {
  VAST_ASSERT(chunk != nullptr);
  auto data = const_cast<char*>(chunk->data()); // CAF won't touch it.
  caf::binary_deserializer source{nullptr, data, chunk->size()};
  return deserialize(source);
}

// -- visitation ---------------------------------------------------------------

void legacy_table_slice::append_column_to_index(size_type col,
                                                value_index& idx) const {
  for (size_type row = 0; row < rows(); ++row)
    idx.append(at(row, col), offset() + row);
}

// -- properties ---------------------------------------------------------------

record_type legacy_table_slice::layout() const noexcept {
  return header_.layout;
}

size_type legacy_table_slice::rows() const noexcept {
  return header_.rows;
}

size_type legacy_table_slice::columns() const noexcept {
  return header_.layout.fields.size();
}

id legacy_table_slice::offset() const noexcept {
  return header_.offset;
}

void legacy_table_slice::offset(id offset) noexcept {
  header_.offset = offset;
}

int legacy_table_slice::instances() {
  return instance_count_;
}

// -- comparison operators -----------------------------------------------------

bool operator==(const legacy_table_slice& x, const legacy_table_slice& y) {
  if (&x == &y)
    return true;
  if (x.rows() != y.rows() || x.columns() != y.columns()
      || x.layout() != y.layout())
    return false;
  for (size_t row = 0; row < x.rows(); ++row)
    for (size_t col = 0; col < x.columns(); ++col)
      if (x.at(row, col) != y.at(row, col))
        return false;
  return true;
}

bool operator!=(const legacy_table_slice& x, const legacy_table_slice& y) {
  return !(x == y);
}

// -- concepts -----------------------------------------------------------------

caf::error inspect(caf::serializer& sink, legacy_table_slice_ptr& ptr) {
  if (!ptr)
    return sink(caf::atom("NULL"));
  return caf::error::eval(
    [&] { return sink(ptr->implementation_id()); },
    [&] { return sink(ptr->layout(), ptr->rows(), ptr->offset()); },
    [&] { return ptr->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, legacy_table_slice_ptr& ptr) {
  caf::atom_value id;
  if (auto err = source(id))
    return err;
  if (id == caf::atom("NULL")) {
    ptr.reset();
    return caf::none;
  }
  table_slice_header header;
  if (auto err = source(header.layout, header.rows, header.offset))
    return err;
  ptr = factory<legacy_table_slice>::make(id, std::move(header));
  if (!ptr)
    return ec::invalid_table_slice_type;
  return ptr.unshared().deserialize(source);
}

// TODO: this function will boil down to accessing the chunk inside the table
// slice and then calling GetTableSlice(buf). But until we touch the table
// slice internals, we use this helper.
caf::expected<flatbuffers::Offset<fbs::FlatTableSlice>>
pack(flatbuffers::FlatBufferBuilder& builder, legacy_table_slice_ptr x) {
  // This local builder instance will vanish once we can access the underlying
  // chunk of a table slice.
  flatbuffers::FlatBufferBuilder local_builder;
  std::vector<char> layout_buffer;
  caf::binary_serializer sink1{nullptr, layout_buffer};
  if (auto error = sink1(x->layout()))
    return error;
  std::vector<char> data_buffer;
  caf::binary_serializer sink2{nullptr, data_buffer};
  if (auto error = sink2(x))
    return error;
  auto transform =
    [](caf::atom_value x) -> caf::expected<fbs::table_slice::legacy::Encoding> {
    if (x == caf::atom("arrow"))
      return fbs::table_slice::legacy::Encoding::Arrow;
    if (x == caf::atom("msgpack"))
      return fbs::table_slice::legacy::Encoding::MessagePack;
    return make_error(ec::unspecified, "unsupported table slice type", x);
  };
  auto encoding = transform(x->implementation_id());
  if (!encoding)
    return encoding.error();
  auto layout_ptr = reinterpret_cast<const uint8_t*>(layout_buffer.data());
  auto layout = local_builder.CreateVector(layout_ptr, layout_buffer.size());
  auto data_ptr = reinterpret_cast<const uint8_t*>(data_buffer.data());
  auto data = local_builder.CreateVector(data_ptr, data_buffer.size());
  fbs::table_slice::legacy::v0Builder legacy_v0_builder{local_builder};
  legacy_v0_builder.add_offset(x->offset());
  legacy_v0_builder.add_rows(x->rows());
  legacy_v0_builder.add_layout(layout);
  legacy_v0_builder.add_encoding(*encoding);
  legacy_v0_builder.add_data(data);
  auto legacy_v0_slice = legacy_v0_builder.Finish();
  fbs::TableSliceBuilder table_slice_builder{local_builder};
  table_slice_builder.add_table_slice_type(
    fbs::table_slice::TableSlice::legacy_v0);
  table_slice_builder.add_table_slice(legacy_v0_slice.Union());
  auto flat_slice = table_slice_builder.Finish();
  local_builder.Finish(flat_slice);
  auto buffer = span<const uint8_t>{local_builder.GetBufferPointer(),
                                    local_builder.GetSize()};
  // This is the only code that will remain. All the stuff above will move into
  // the respective table slice builders.
  auto bytes = builder.CreateVector(buffer.data(), buffer.size());
  fbs::FlatTableSliceBuilder flat_table_slice_builder{builder};
  flat_table_slice_builder.add_data(bytes);
  return flat_table_slice_builder.Finish();
}

// TODO: The dual to the note above applies here.
caf::error
unpack(const fbs::table_slice::legacy::v0& x, legacy_table_slice_ptr& y) {
  auto ptr = reinterpret_cast<const char*>(x.data()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.data()->size()};
  return source(y);
}

// -- intrusive_ptr facade -----------------------------------------------------

void intrusive_ptr_add_ref(const legacy_table_slice* ptr) {
  intrusive_ptr_add_ref(static_cast<const caf::ref_counted*>(ptr));
}

void intrusive_ptr_release(const legacy_table_slice* ptr) {
  intrusive_ptr_release(static_cast<const caf::ref_counted*>(ptr));
}

legacy_table_slice* intrusive_cow_ptr_unshare(legacy_table_slice*& ptr) {
  return caf::default_intrusive_cow_ptr_unshare(ptr);
}

// -- operators ----------------------------------------------------------------

void select(std::vector<table_slice>& result, const table_slice& xs,
            const ids& selection) {
  VAST_ASSERT(xs.encoding() != table_slice::encoding::none);
  auto xs_ids = make_ids({{xs.offset(), xs.offset() + xs.rows()}});
  auto intersection = selection & xs_ids;
  auto intersection_rank = rank(intersection);
  // Do no rows qualify?
  if (intersection_rank == 0)
    return;
  // Do all rows qualify?
  if (rank(xs_ids) == intersection_rank) {
    result.emplace_back(xs);
    return;
  }
  // Start slicing and dicing.
  caf::atom_value impl;
  switch (xs.encoding()) {
    case table_slice::encoding::none:
      impl = caf::atom("NULL");
      break;
    case table_slice::encoding::arrow:
      impl = caf::atom("arrow");
      break;
    case table_slice::encoding::msgpack:
      impl = caf::atom("msgpack");
      break;
  }
  auto builder = factory<table_slice_builder>::make(impl, xs.layout());
  if (builder == nullptr) {
    VAST_ERROR(__func__, "failed to get a table slice builder for", impl);
    return;
  }
  id last_offset = xs.offset();
  auto push_slice = [&] {
    if (builder->rows() == 0)
      return;
    auto slice = builder->finish();
    if (slice.encoding() == table_slice::encoding::none) {
      VAST_WARNING(__func__, "got an empty slice");
      return;
    }
    slice.offset(last_offset);
    result.emplace_back(std::move(slice));
  };
  auto last_id = last_offset - 1;
  for (auto id : select(intersection)) {
    // Finish last slice when hitting non-consecutive IDs.
    if (last_id + 1 != id) {
      push_slice();
      last_offset = id;
      last_id = id;
    } else {
      ++last_id;
    }
    VAST_ASSERT(id >= xs.offset());
    auto row = id - xs.offset();
    VAST_ASSERT(row < xs.rows());
    for (size_t column = 0; column < xs.columns(); ++column) {
      auto cell_value = xs.at(row, column);
      if (!builder->add(cell_value)) {
        VAST_ERROR(__func__, "failed to add data at column", column, "in row",
                   row, "to the builder:", cell_value);
        return;
      }
    }
  }
  push_slice();
}

std::vector<table_slice> select(const table_slice& xs, const ids& selection) {
  std::vector<table_slice> result;
  select(result, xs, selection);
  return result;
}

table_slice truncate(const table_slice& slice, size_t num_rows) {
  VAST_ASSERT(slice.encoding() != table_slice::encoding::none);
  VAST_ASSERT(num_rows > 0);
  if (slice.rows() <= num_rows)
    return slice;
  auto selection = make_ids({{slice.offset(), slice.offset() + num_rows}});
  auto xs = select(slice, selection);
  VAST_ASSERT(xs.size() == 1);
  return std::move(xs.back());
}

std::pair<table_slice, table_slice>
split(const table_slice& slice, size_t partition_point) {
  VAST_ASSERT(slice.encoding() != table_slice::encoding::none);
  if (partition_point == 0)
    return {{}, slice};
  if (partition_point >= slice.rows())
    return {slice, {}};
  auto first = slice.offset();
  auto mid = first + partition_point;
  auto last = first + slice.rows();
  // Create first table slice.
  auto xs = select(slice, make_ids({{first, mid}}));
  VAST_ASSERT(xs.size() == 1);
  // Create second table slice.
  select(xs, slice, make_ids({{mid, last}}));
  VAST_ASSERT(xs.size() == 2);
  return {std::move(xs.front()), std::move(xs.back())};
}

uint64_t rows(const std::vector<table_slice>& slices) {
  auto result = uint64_t{0};
  for (auto& slice : slices)
    result += slice.rows();
  return result;
}

namespace {

struct row_evaluator {
  row_evaluator(const table_slice& slice, size_t row)
    : slice_{slice}, row_{row} {
    // nop
  }

  template <class T>
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  bool operator()(caf::none_t) {
    return false;
  }

  bool operator()(const conjunction& c) {
    for (auto& op : c)
      if (!caf::visit(*this, op))
        return false;
    return true;
  }

  bool operator()(const disjunction& d) {
    for (auto& op : d)
      if (caf::visit(*this, op))
        return true;
    return false;
  }

  bool operator()(const negation& n) {
    return !caf::visit(*this, n.expr());
  }

  bool operator()(const predicate& p) {
    op_ = p.op;
    return caf::visit(*this, p.lhs, p.rhs);
  }

  bool operator()(const attribute_extractor& e, const data& d) {
    // TODO: Transform this AST node into a constant-time lookup node (e.g.,
    // data_extractor). It's not necessary to iterate over the schema for every
    // row; this should happen upfront.
    auto layout = slice_.layout();
    if (e.attr == atom::type_v)
      return evaluate(layout.name(), op_, d);
    if (e.attr == atom::timestamp_v) {
      for (size_t col = 0; col < layout.fields.size(); ++col) {
        auto& field = layout.fields[col];
        if (has_attribute(field.type, "timestamp")) {
          if (!caf::holds_alternative<time_type>(field.type)) {
            VAST_WARNING_ANON("got timestamp attribute for non-time type");
            return false;
          }
        }
        auto lhs = to_canonical(field.type, slice_.at(row_, col));
        auto rhs = make_view(d);
        return evaluate_view(lhs, op_, rhs);
      }
    }
    return false;
  }

  bool operator()(const type_extractor&, const data&) {
    die("type extractor should have been resolved at this point");
  }

  bool operator()(const field_extractor&, const data&) {
    die("field extractor should have been resolved at this point");
  }

  bool operator()(const data_extractor& e, const data& d) {
    VAST_ASSERT(e.offset.size() == 1);
    auto layout = slice_.layout();
    if (e.type != layout) // TODO: make this a precondition instead.
      return false;
    auto col = e.offset[0];
    auto& field = layout.fields[col];
    auto lhs = to_canonical(field.type, slice_.at(row_, col));
    auto rhs = make_data_view(d);
    return evaluate_view(lhs, op_, rhs);
  }

  const table_slice& slice_;
  size_t row_;
  relational_operator op_;
};

} // namespace

ids evaluate(const expression& expr, const table_slice& slice) {
  // TODO: switch to a column-based evaluation strategy where it makes sense.
  ids result;
  result.append(false, slice.offset());
  for (size_t row = 0; row != slice.rows(); ++row) {
    auto x = caf::visit(row_evaluator{slice, row}, expr);
    result.append_bit(x);
  }
  return result;
}

} // namespace vast
