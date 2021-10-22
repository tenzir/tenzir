//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/msgpack_table_slice.hpp"

#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/die.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/msgpack.hpp"
#include "vast/value_index.hpp"

#include <type_traits>

namespace vast {

// -- utility functions --------------------------------------------------------

namespace {

class msgpack_array_view : public container_view<data_view>,
                           detail::totally_ordered<msgpack_array_view> {
public:
  msgpack_array_view(legacy_type value_type, msgpack::array_view xs)
    : size_{xs.size()}, value_type_{std::move(value_type)}, data_{xs.data()} {
    // nop
  }

  // implemented out-of-line below due to dependency on decode
  value_type at(size_type i) const override;

  size_type size() const noexcept override {
    return size_;
  }

private:
  size_t size_;
  legacy_type value_type_;
  msgpack::overlay data_;
};

class msgpack_map_view : public container_view<std::pair<data_view, data_view>>,
                         detail::totally_ordered<msgpack_map_view> {
public:
  msgpack_map_view(legacy_type key_type, legacy_type value_type,
                   msgpack::array_view xs)
    : size_{xs.size() / 2},
      key_type_{std::move(key_type)},
      value_type_{std::move(value_type)},
      data_{xs.data()} {
    using namespace msgpack;
    VAST_ASSERT(is_fixmap(xs.format()) || xs.format() == map16
                || xs.format() == map32);
  }

  // implemented out-of-line below due to dependency on decode
  value_type at(size_type i) const override;

  size_type size() const noexcept override {
    return size_;
  }

private:
  size_t size_;
  legacy_type key_type_;
  legacy_type value_type_;
  msgpack::overlay data_;
};

// Helper utilities for decoding.

struct identity {
  template <class T>
  auto operator()(T x) const {
    return x;
  }
};

template <class To>
struct converter {
  template <class T>
  auto operator()(T x) const {
    return To{x};
  }
};

template <class T, class F = identity>
auto make_data_view_lambda(F f = {}) {
  return [=](T x) { return make_data_view(f(x)); };
}

auto make_none_view() {
  return [](auto) { return data_view{}; };
}

template <class F = identity>
auto make_signed_visitor(F f = {}) {
  auto g = [=](auto x) {
    using signed_type = std::make_signed_t<decltype(x)>;
    return f(static_cast<signed_type>(x));
  };
  return detail::overload{
    make_data_view_lambda<uint8_t>(g), // for 0
    make_data_view_lambda<int8_t>(g),
    make_data_view_lambda<int16_t>(g),
    make_data_view_lambda<int32_t>(g),
    make_data_view_lambda<int64_t>(g),
    make_none_view(),
  };
}

template <class F = identity>
auto make_unsigned_visitor(F f = {}) {
  return detail::overload{
    make_data_view_lambda<uint8_t>(f),
    make_data_view_lambda<uint16_t>(f),
    make_data_view_lambda<uint32_t>(f),
    make_data_view_lambda<uint64_t>(f),
    make_none_view(),
  };
}

// Decodes a data view from one or more objects.
data_view decode(msgpack::overlay& objects, const legacy_type& t);

template <class T>
data_view decode(msgpack::overlay& objects, const T& t) {
  using namespace msgpack;
  auto o = objects.get();
  if (o.format() == nil)
    return {};
  if constexpr (std::is_same_v<T, legacy_none_type>) {
    // This branch should never get triggered because an object with format
    // 'nil' is handled already above.
    VAST_ASSERT(!"null check too late");
    return {};
  } else if constexpr (std::is_same_v<T, legacy_bool_type>) {
    if (auto x = get<bool>(o))
      return make_data_view(*x);
  } else if constexpr (std::is_same_v<T, legacy_integer_type>) {
    auto to_integer = [](auto x) { return integer{x}; };
    auto f = make_signed_visitor(to_integer);
    return visit(f, o);
  } else if constexpr (std::is_same_v<T, legacy_count_type>) {
    auto f = make_unsigned_visitor<converter<count>>();
    return visit(f, o);
  } else if constexpr (std::is_same_v<T, legacy_real_type>) {
    if (auto x = get<double>(o))
      return make_data_view(*x);
  } else if constexpr (std::is_same_v<T, legacy_duration_type>) {
    using namespace std::chrono;
    auto to_ns = [](auto x) { return duration{nanoseconds{x}}; };
    auto f = make_signed_visitor(to_ns);
    return visit(f, o);
  } else if constexpr (std::is_same_v<T, legacy_time_type>) {
    using namespace std::chrono;
    auto to_ts = [](auto x) { return time{duration{nanoseconds{x}}}; };
    auto f = make_signed_visitor(to_ts);
    return visit(f, o);
  } else if constexpr (std::is_same_v<T, legacy_string_type>) {
    if (auto x = get<std::string_view>(o))
      return make_data_view(*x);
  } else if constexpr (std::is_same_v<T, legacy_pattern_type>) {
    if (auto x = get<std::string_view>(o))
      return data_view{pattern_view{*x}};
  } else if constexpr (std::is_same_v<T, legacy_address_type>) {
    if (auto x = get<std::string_view>(o)) {
      VAST_ASSERT(x->size() == 4 || x->size() == 16);
      auto addr = x->size() == 4
                    ? address::v4(std::span<const char, 4>{x->data(), 4})
                    : address::v6(std::span<const char, 16>{x->data(), 16});
      return make_data_view(addr);
    }
  } else if constexpr (std::is_same_v<T, legacy_subnet_type>) {
    if (auto xs = get<array_view>(o)) {
      VAST_ASSERT(xs->size() == 2);
      auto inner = xs->data();
      auto str = *get<std::string_view>(inner.get());
      auto addr = str.size() == 4
                    ? address::v4(std::span<const char, 4>{str.data(), 4})
                    : address::v6(std::span<const char, 16>{str.data(), 16});
      inner.next();
      auto length = *get<uint8_t>(inner.get());
      return data_view{view<subnet>{make_view(addr), length}};
    }
  } else if constexpr (std::is_same_v<T, legacy_enumeration_type>) {
    if (auto x = get<enumeration>(o))
      return make_data_view(*x);
  } else if constexpr (std::is_same_v<T, legacy_list_type>) {
    if (auto xs = get<array_view>(o)) {
      auto ptr = caf::make_counted<msgpack_array_view>(t.value_type, *xs);
      return list_view_handle{list_view_ptr{std::move(ptr)}};
    }
  } else if constexpr (std::is_same_v<T, legacy_map_type>) {
    if (auto xs = get<array_view>(o)) {
      auto ptr
        = caf::make_counted<msgpack_map_view>(t.key_type, t.value_type, *xs);
      return map_view_handle{map_view_ptr{std::move(ptr)}};
    }
  } else if constexpr (std::is_same_v<T, legacy_record_type>) {
    VAST_ASSERT(!"records are unrolled");
    return {};
  } else if constexpr (std::is_same_v<T, legacy_alias_type>) {
    return decode(objects, t.value_type);
  } else {
    static_assert(detail::always_false_v<T>, "missing type");
  }
  // The end of this function is unreachable.
  vast::die("unreachable");
}

data_view decode(msgpack::overlay& objects, const legacy_type& t) {
  // Dispatch to the more specific decode.
  return caf::visit(
    [&](auto&& x) { return decode(objects, std::forward<decltype(x)>(x)); }, t);
}

msgpack_array_view::value_type msgpack_array_view::at(size_type i) const {
  VAST_ASSERT(i < size());
  auto xs = data_;
  xs.next(i);
  return decode(xs, value_type_);
}

msgpack_map_view::value_type msgpack_map_view::at(size_type i) const {
  VAST_ASSERT(i < size());
  auto xs = data_;
  xs.next(i * 2);
  auto key = decode(xs, key_type_);
  auto n = xs.next();
  VAST_ASSERT(n > 0);
  auto value = decode(xs, value_type_);
  return {std::move(key), std::move(value)};
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

template <class FlatBuffer>
msgpack_table_slice<FlatBuffer>::msgpack_table_slice(
  const FlatBuffer& slice) noexcept
  : slice_{slice}, state_{} {
  if (auto err = fbs::deserialize_bytes(slice_.layout(), state_.layout))
    die("failed to deserialize layout: " + render(err));
  state_.columns = state_.layout.num_leaves();
}

template <class FlatBuffer>
msgpack_table_slice<FlatBuffer>::msgpack_table_slice(
  const FlatBuffer& slice, legacy_record_type layout) noexcept
  : slice_{slice}, state_{} {
  state_.layout = std::move(layout);
  state_.columns = state_.layout.num_leaves();
}

template <class FlatBuffer>
msgpack_table_slice<FlatBuffer>::~msgpack_table_slice() noexcept {
  // nop
}

// -- properties -------------------------------------------------------------

template <class FlatBuffer>
const legacy_record_type&
msgpack_table_slice<FlatBuffer>::layout() const noexcept {
  return state_.layout;
}

template <class FlatBuffer>
table_slice::size_type msgpack_table_slice<FlatBuffer>::rows() const noexcept {
  return slice_.offset_table()->size();
}

template <class FlatBuffer>
table_slice::size_type
msgpack_table_slice<FlatBuffer>::columns() const noexcept {
  return state_.columns;
}

// -- data access ------------------------------------------------------------

template <class FlatBuffer>
void msgpack_table_slice<FlatBuffer>::append_column_to_index(
  id offset, table_slice::size_type column, value_index& index) const {
  const auto& offset_table = *slice_.offset_table();
  auto view = as_bytes(*slice_.data());
  auto layout_offset = state_.layout.offset_from_index(column);
  VAST_ASSERT(layout_offset);
  auto type = state_.layout.at(*layout_offset)->type;
  for (size_t row = 0; row < rows(); ++row) {
    auto row_offset = offset_table[row];
    auto xs = msgpack::overlay{view.subspan(row_offset)};
    xs.next(column);
    auto x = decode(xs, type);
    index.append(std::move(x), offset + row);
  }
}

template <class FlatBuffer>
data_view
msgpack_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                    table_slice::size_type column) const {
  const auto& offset_table = *slice_.offset_table();
  auto view = as_bytes(*slice_.data());
  // First find the desired row...
  VAST_ASSERT(row < offset_table.size());
  auto offset = offset_table[row];
  VAST_ASSERT(offset < static_cast<size_t>(view.size()));
  auto xs = msgpack::overlay{view.subspan(offset)};
  // ...then skip (decode) up to the desired column.
  xs.next(column);
  auto layout_offset = state_.layout.offset_from_index(column);
  VAST_ASSERT(layout_offset);
  return decode(xs, state_.layout.at(*layout_offset)->type);
}

template <class FlatBuffer>
data_view msgpack_table_slice<FlatBuffer>::at(table_slice::size_type row,
                                              table_slice::size_type column,
                                              const legacy_type& t) const {
  const auto& offset_table = *slice_.offset_table();
  auto view = as_bytes(*slice_.data());
  // First find the desired row...
  VAST_ASSERT(row < offset_table.size());
  VAST_ASSERT(congruent(
    state_.layout.at(*state_.layout.offset_from_index(column))->type, t));
  auto offset = offset_table[row];
  VAST_ASSERT(offset < static_cast<size_t>(view.size()));
  auto xs = msgpack::overlay{view.subspan(offset)};
  // ...then skip (decode) up to the desired column.
  xs.next(column);
  return decode(xs, t);
}

// -- template machinery -------------------------------------------------------

/// Explicit template instantiations for all MessagePack encoding versions.
template class msgpack_table_slice<fbs::table_slice::msgpack::v0>;

} // namespace vast
