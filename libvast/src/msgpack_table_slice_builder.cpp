// Copyright Tenzir GmbH. All rights reserved.

#include "vast/msgpack_table_slice_builder.hpp"

#include "vast/msgpack_table_slice.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/make_counted.hpp>

#include <memory>

#include <vast/detail/overload.hpp>

using namespace vast;

namespace vast {

caf::atom_value msgpack_table_slice_builder::get_implementation_id() noexcept {
  return msgpack_table_slice::class_id;
}

table_slice_builder_ptr
msgpack_table_slice_builder::make(record_type layout,
                                  size_t initial_buffer_size) {
  return caf::make_counted<msgpack_table_slice_builder>(std::move(layout),
                                                        initial_buffer_size);
}

msgpack_table_slice_builder::msgpack_table_slice_builder(
  record_type layout, size_t initial_buffer_size)
  : super{std::move(layout)}, col_{0}, builder_{buffer_} {
  buffer_.reserve(initial_buffer_size);
}

msgpack_table_slice_builder::~msgpack_table_slice_builder() {
  // nop
}

namespace {

template <class Builder>
size_t encode(Builder& builder, data_view v);

template <class Builder, class View>
size_t encode(Builder& builder, View v) {
  using namespace msgpack;
  auto proxy_encode = [](auto& proxy, data_view x) { return encode(proxy, x); };
  auto f = detail::overload(
    [&](auto x) { return put(builder, x); },
    [&](view<duration> x) { return put(builder, x.count()); },
    [&](view<time> x) { return put(builder, x.time_since_epoch().count()); },
    [&](view<pattern> x) { return put(builder, x.string()); },
    [&](view<address> x) {
      auto bytes = x.data();
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
      auto n0 = encode(builder, make_view(x.network()));
      if (n0 == 0)
        return n0;
      auto n1 = put(builder, x.length());
      if (n1 == 0)
        return n1;
      return n0 + n1;
    },
    [&](view<port> x) {
      return put(builder, static_cast<uint8_t>(x.type()), x.number());
    },
    [&](view<enumeration> x) {
      // The noop cast exists only to communicate the MsgPack type.
      return put(builder, static_cast<uint8_t>(x));
    },
    [&](view<vector> xs) { return put_array(builder, *xs, proxy_encode); },
    [&](view<set> xs) { return put_array(builder, *xs, proxy_encode); },
    [&](view<map> xs) { return put_map(builder, *xs, proxy_encode); });
  return f(v);
}

template <class Builder>
size_t encode(Builder& builder, data_view v) {
  return caf::visit([&](auto&& x) { return encode(builder, x); }, v);
}

} // namespace

bool msgpack_table_slice_builder::add_impl(data_view x) {
  // Check whether input is valid.
  if (!type_check(layout().fields[col_].type, x))
    return false;
  if (col_ == 0)
    offset_table_.push_back(buffer_.size());
  col_ = (col_ + 1) % columns();
  auto f = [&](auto v) { return encode(builder_, v); };
  auto n = caf::visit(f, x);
  VAST_ASSERT(n > 0);
  return true;
}

table_slice_ptr msgpack_table_slice_builder::finish() {
  // Sanity check.
  if (col_ != 0)
    return nullptr;
  table_slice_header header;
  header.layout = layout();
  header.rows = offset_table_.size();
  auto ptr = new msgpack_table_slice{std::move(header)};
  ptr->offset_table_ = std::move(offset_table_);
  ptr->chunk_ = chunk::make(std::move(buffer_));
  ptr->buffer_ = as_bytes(span{ptr->chunk_->data(), ptr->chunk_->size()});
  offset_table_ = {};
  buffer_ = {};
  return table_slice_ptr{ptr, false};
}

size_t msgpack_table_slice_builder::rows() const noexcept {
  return offset_table_.size();
}

caf::atom_value
msgpack_table_slice_builder::implementation_id() const noexcept {
  return get_implementation_id();
}

} // namespace vast
