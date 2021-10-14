//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/msgpack_table_slice_builder.hpp"

#include "vast/detail/overload.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/msgpack_table_slice.hpp"

#include <cstddef>
#include <memory>

namespace vast {

// -- utility functions --------------------------------------------------------

namespace {

template <class Builder, class View>
size_t encode(Builder& builder, View v) {
  using namespace msgpack;
  auto f = detail::overload{
    [&](auto x) {
      return put(builder, x);
    },
    [&](view<duration> x) {
      return put(builder, x.count());
    },
    [&](view<time> x) {
      return put(builder, x.time_since_epoch().count());
    },
    [&](view<pattern> x) {
      return put(builder, x.string());
    },
    [&](view<address> x) {
      auto bytes = as_bytes(x);
      auto ptr = reinterpret_cast<const char*>(bytes.data());
      if (x.is_v4()) {
        auto str = std::string_view{ptr + 12, 4};
        return builder.template add<fixstr>(str);
      } else {
        auto str = std::string_view{ptr, 16};
        return builder.template add<fixstr>(str);
      }
    },
    [&](view<subnet> x) {
      auto proxy = builder.template build<fixarray>();
      auto n = encode(proxy, make_view(x.network()));
      if (n == 0) {
        builder.reset();
        return n;
      }
      proxy.template add<uint8>(x.length());
      return builder.add(std::move(proxy));
    },
    [&](view<enumeration> x) {
      // The noop cast exists only to communicate the MsgPack type.
      return put(builder, static_cast<uint8_t>(x));
    },
    [&](view<list> xs) {
      return put_array(builder, *xs);
    },
    [&](view<map> xs) {
      return put_map(builder, *xs);
    },
    [&](view<record> xs) -> size_t {
      // We store records flattened, so we just append all values sequentially.
      size_t result = 0;
      for ([[maybe_unused]] auto [_, x] : xs) {
        auto n = put(builder, x);
        if (n == 0) {
          builder.reset();
          return 0;
        }
        result += n;
      }
      return result;
    },
  };
  return f(v);
}

} // namespace

namespace msgpack {

// We activate ADL for data_view here so that we can rely on existing recursive
// put functions defined in msgpack_builder.hpp.
template <class Builder>
[[nodiscard]] size_t put(Builder& builder, data_view v) {
  return caf::visit([&](auto&& x) { return encode(builder, x); }, v);
}

} // namespace msgpack

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder_ptr
msgpack_table_slice_builder::make(legacy_record_type layout,
                                  size_t initial_buffer_size) {
  return table_slice_builder_ptr{
    new msgpack_table_slice_builder{std::move(layout), initial_buffer_size},
    false};
}

msgpack_table_slice_builder::~msgpack_table_slice_builder() {
  // nop
}

// -- properties ---------------------------------------------------------------

size_t msgpack_table_slice_builder::columns() const noexcept {
  return flat_layout_.fields.size();
}

table_slice msgpack_table_slice_builder::finish(
  std::span<const std::byte> serialized_layout) {
  // Sanity check: If this triggers, the calls to add() did not match the number
  // of fields in the layout.
  VAST_ASSERT(column_ == 0);
  // Pack layout.
  auto use_layout = [&](const auto& buf) {
    return builder_.CreateVector(
      reinterpret_cast<const unsigned char*>(buf.data()), buf.size());
  };
  auto gen_layout = [&]() {
    caf::binary_serializer source(nullptr, serialized_layout_cache_);
    auto error = source(layout());
    VAST_ASSERT(error == caf::no_error);
    return use_layout(serialized_layout_cache_);
  };
  auto layout_buffer = !serialized_layout.empty()
                         ? use_layout(serialized_layout)
                         : (!serialized_layout_cache_.empty()
                              ? use_layout(serialized_layout_cache_)
                              : gen_layout());
  // Pack offset table.
  auto offset_table_buffer = builder_.CreateVector(offset_table_);
  // Pack data.
  auto data_buffer = builder_.CreateVector(
    reinterpret_cast<const uint8_t*>(data_.data()), data_.size());
  // Create MessagePack-encoded table slices.
  auto msgpack_table_slice_buffer = fbs::table_slice::msgpack::Createv0(
    builder_, layout_buffer, offset_table_buffer, data_buffer);
  // Create and finish table slice.
  auto table_slice_buffer
    = fbs::CreateTableSlice(builder_, fbs::table_slice::TableSlice::msgpack_v0,
                            msgpack_table_slice_buffer.Union());
  fbs::FinishTableSliceBuffer(builder_, table_slice_buffer);
  // Reset the builder state.
  offset_table_ = {};
  data_ = {};
  msgpack_builder_.reset();
  // Create the table slice from the chunk.
  auto chunk = fbs::release(builder_);
  return table_slice{std::move(chunk), table_slice::verify::no, layout()};
}

size_t msgpack_table_slice_builder::rows() const noexcept {
  return offset_table_.size();
}

table_slice_encoding
msgpack_table_slice_builder::implementation_id() const noexcept {
  return table_slice_encoding::arrow;
}

void msgpack_table_slice_builder::reserve(size_t num_rows) {
  offset_table_.reserve(num_rows);
}

// -- implementation details ---------------------------------------------------

msgpack_table_slice_builder::msgpack_table_slice_builder(
  legacy_record_type layout, size_t initial_buffer_size)
  : table_slice_builder{std::move(layout)},
    flat_layout_{flatten(this->layout())},
    msgpack_builder_{data_},
    builder_{initial_buffer_size} {
  data_.reserve(initial_buffer_size);
}

bool msgpack_table_slice_builder::add_impl(data_view x) {
  // Check whether input is valid.
  if (!type_check(flat_layout_.fields[column_].type, x))
    return false;
  if (column_ == 0)
    offset_table_.push_back(data_.size());
  column_ = (column_ + 1) % columns();
  auto n = put(msgpack_builder_, x);
  VAST_ASSERT(n > 0);
  return true;
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all MessagePack encoding versions.
template class msgpack_table_slice<fbs::table_slice::msgpack::v0>;

} // namespace vast
