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

#include "vast/error.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/table_slice_header.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

namespace vast::fbs {

caf::expected<Encoding> create_encoding(caf::atom_value x) {
  if (x == caf::atom("default"))
    return Encoding::CAF;
  if (x == caf::atom("arrow"))
    return Encoding::Arrow;
  if (x == caf::atom("msgpack"))
    return Encoding::MessagePack;
  return make_error(ec::unspecified, "unsupported table slice type", x);
}

caf::atom_value make_encoding(Encoding x) {
  switch (x) {
    case Encoding::CAF:
      return caf::atom("default");
    case Encoding::Arrow:
      return caf::atom("arrow");
    case Encoding::MessagePack:
      return caf::atom("msgpack");
  }
}

// TODO: this function will boil down to accessing the chunk inside the table
// slice and then calling GetTableSlice(buf). But until we touch the table
// slice internals, we use this helper.
caf::expected<flatbuffers::Offset<TableSlice>>
create_table_slice(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x) {
  std::vector<char> layout_buffer;
  caf::binary_serializer sink1{nullptr, layout_buffer};
  if (auto error = sink1(x->layout()))
    return error;
  std::vector<char> data_buffer;
  caf::binary_serializer sink2{nullptr, data_buffer};
  if (auto error = sink2(x))
    return error;
  auto encoding = create_encoding(x->implementation_id());
  if (!encoding)
    return encoding.error();
  auto layout_ptr = reinterpret_cast<const uint8_t*>(layout_buffer.data());
  auto layout = builder.CreateVector(layout_ptr, layout_buffer.size());
  auto data_ptr = reinterpret_cast<const uint8_t*>(data_buffer.data());
  auto data = builder.CreateVector(data_ptr, data_buffer.size());
  TableSliceBuilder table_slice_builder{builder};
  table_slice_builder.add_offset(x->offset());
  table_slice_builder.add_rows(x->rows());
  table_slice_builder.add_layout(layout);
  table_slice_builder.add_encoding(*encoding);
  table_slice_builder.add_data(data);
  return table_slice_builder.Finish();
}

// TODO: The dual to the note above applies here.
table_slice_ptr make_table_slice(const TableSlice& x) {
  table_slice_ptr result;
  auto ptr = reinterpret_cast<const char*>(x.data()->Data());
  caf::binary_deserializer source{nullptr, ptr, x.data()->size()};
  if (auto error = source(result))
    return nullptr;
  return result;
}

} // namespace vast::fbs
