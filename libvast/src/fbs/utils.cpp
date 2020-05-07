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

#include "vast/fbs/utils.hpp"

#include "vast/chunk.hpp"
#include "vast/error.hpp"
#include "vast/meta_index.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/table_slice_header.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace vast::fbs {

namespace {

// This function will eventually vanish, because the respective builders will
// be the only components that write a (hardcoded) instance of Encoding.
caf::expected<Encoding> transform(caf::atom_value x) {
  if (x == caf::atom("default"))
    return Encoding::CAF;
  if (x == caf::atom("arrow"))
    return Encoding::Arrow;
  if (x == caf::atom("msgpack"))
    return Encoding::MessagePack;
  return make_error(ec::unspecified, "unsupported table slice type", x);
}

} // namespace

chunk_ptr release(flatbuffers::FlatBufferBuilder& builder) {
  size_t offset;
  size_t size;
  auto ptr = builder.ReleaseRaw(size, offset);
  auto deleter = [=]() { flatbuffers::DefaultAllocator::dealloc(ptr, size); };
  return chunk::make(size - offset, ptr + offset, deleter);
}

flatbuffers::Verifier make_verifier(chunk_ptr chk) {
  VAST_ASSERT(chk != nullptr);
  return make_verifier(as_bytes(chk));
}

flatbuffers::Verifier make_verifier(span<const byte> xs) {
  auto data = reinterpret_cast<const uint8_t*>(xs.data());
  return flatbuffers::Verifier{data, xs.size()};
}

caf::error check_version(Version given, Version expected) {
  if (given == expected)
    return caf::none;
  return make_error(ec::version_error, "unsupported version;", "got", given,
                    ", expected", expected);
}

span<const byte> as_bytes(const flatbuffers::FlatBufferBuilder& builder) {
  auto data = reinterpret_cast<const byte*>(builder.GetBufferPointer());
  auto size = builder.GetSize();
  return {data, size};
}

// TODO: this function will boil down to accessing the chunk inside the table
// slice and then calling GetTableSlice(buf). But until we touch the table
// slice internals, we use this helper.
caf::expected<flatbuffers::Offset<TableSliceBuffer>>
pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x) {
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
  auto encoding = transform(x->implementation_id());
  if (!encoding)
    return encoding.error();
  auto layout_ptr = reinterpret_cast<const uint8_t*>(layout_buffer.data());
  auto layout = local_builder.CreateVector(layout_ptr, layout_buffer.size());
  auto data_ptr = reinterpret_cast<const uint8_t*>(data_buffer.data());
  auto data = local_builder.CreateVector(data_ptr, data_buffer.size());
  TableSliceBuilder table_slice_builder{local_builder};
  table_slice_builder.add_offset(x->offset());
  table_slice_builder.add_rows(x->rows());
  table_slice_builder.add_layout(layout);
  table_slice_builder.add_encoding(*encoding);
  table_slice_builder.add_data(data);
  auto flat_slice = table_slice_builder.Finish();
  local_builder.Finish(flat_slice);
  auto buffer = span<const uint8_t>{local_builder.GetBufferPointer(),
                                    local_builder.GetSize()};
  // This is the only code that will remain. All the stuff above will move into
  // the respective table slice builders.
  auto bytes = builder.CreateVector(buffer.data(), buffer.size());
  TableSliceBufferBuilder table_slice_buffer_builder{builder};
  table_slice_buffer_builder.add_data(bytes);
  return table_slice_buffer_builder.Finish();
}

caf::expected<caf::atom_value> unpack(Encoding x) {
  switch (x) {
    case Encoding::CAF:
      return caf::atom("default");
    case Encoding::Arrow:
      return caf::atom("arrow");
    case Encoding::MessagePack:
      return caf::atom("msgpack");
  }
  return make_error(ec::unspecified, "unsupported Encoding", x);
}

// TODO: The dual to the note above applies here.
caf::expected<table_slice_ptr> unpack(const TableSlice& x) {
  table_slice_ptr result;
  auto ptr = reinterpret_cast<const char*>(x.data()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.data()->size()};
  if (auto error = source(result))
    return error;
  return result;
}

caf::expected<flatbuffers::Offset<MetaIndex>>
pack(flatbuffers::FlatBufferBuilder& builder, const meta_index& x) {
  std::vector<char> buffer;
  caf::binary_serializer sink{nullptr, buffer};
  if (auto error = sink(x))
    return error;
  auto data_ptr = reinterpret_cast<const uint8_t*>(buffer.data());
  auto data = builder.CreateVector(data_ptr, buffer.size());
  MetaIndexBuilder meta_index_builder{builder};
  meta_index_builder.add_state(data);
  return meta_index_builder.Finish();
}

caf::expected<meta_index> unpack(const MetaIndex& x) {
  meta_index result;
  auto ptr = reinterpret_cast<const char*>(x.state()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.state()->size()};
  if (auto error = source(result))
    return error;
  return result;
}

} // namespace vast::fbs
