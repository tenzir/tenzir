//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/narrow.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <arrow/array.h>

namespace tenzir {

class record_view3;
class list_view3;

template <class T>
struct view_trait3 {
  using type = view<T>;
};

template <>
struct view_trait3<record> {
  using type = record_view3;
};

template <>
struct view_trait3<list> {
  using type = list_view3;
};

template <class T>
using view3 = view_trait3<T>::type;

template <class T>
struct derive_data_view {
  static constexpr auto value = not concepts::one_of<T, pattern, map>;
};

using data_view3_owning_types
  = detail::tl_filter_t<data::types, derive_data_view>;

using data_view_viewing_types
  = detail::tl_map_t<data_view3_owning_types, view_trait3>;

template <typename T>
concept data_view3_type = detail::tl_contains_v<data_view_viewing_types, T>;

/// There is two relations for `data_view3`: Partial and Weak, with generally
/// shared sematics. Weak ordering only exists to be able to use it for sorting,
/// which requires weak ordering.
///
/// * Null compares greater than any value, moving it to the end of a sort
/// * NaN compares greater than any value, moving it to the end of a sort
/// * Numbers are compared across types
/// * Other values are compared as expected, potentially as unordered.
/// * Lists are compared lexicographically, using this ordering on all values.
/// * Records are compared by their sorted keys and respective values.
///   * On matching keys, the values are compared.
///   * On a key mismatch, the ordering is the lexicographic ordering of the keys
///
/// For the weak ordering,
/// * Unordered objects of the same type are considered equivalent. This is of
///   course not correct, but good enough for our purpose (sorting)
/// * Objects of unrelated types are sorted by their type index.

/// Establishes a partial ordering on data. See above for details.
auto partial_order(data_view3 l, const data& r) -> std::partial_ordering;
auto partial_order(data_view3 l, data_view3 r) -> std::partial_ordering;

/// Establishes a weak ordering, suitable for usage with sorting algorithms. See
/// above for details.
auto weak_order(data_view3 l, data_view3 r) -> std::weak_ordering;
auto weak_order(data_view3 l, const data& r) -> std::weak_ordering;

template <>
struct view_trait3<data> {
  using type = data_view3;
};

class record_view3 {
public:
  class iterator {
  public:
    using difference_type = void;
    using value_type = std::pair<std::string_view, data_view3>;
    using pointer = void;
    using reference = value_type&;
    using iterator_category = std::input_iterator_tag;

    const arrow::StructArray* array;
    int64_t index;
    int field;

    auto operator*() const -> value_type;

    auto operator++() -> iterator& {
      TENZIR_ASSERT(field < array->num_fields());
      ++field;
      return *this;
    }

    auto operator==(iterator other) const -> bool {
      TENZIR_ASSERT(array == other.array);
      TENZIR_ASSERT(index == other.index);
      return field == other.field;
    }

    auto operator!=(iterator other) const -> bool {
      return ! (*this == other);
    }
  };

  static auto from_valid(const arrow::StructArray& array, int64_t index)
    -> record_view3 {
    return record_view3{array, index};
  }

  auto begin() const -> iterator {
    return iterator{&array_, index_, 0};
  }

  auto end() const -> iterator {
    return iterator{&array_, index_, array_.num_fields()};
  }

  template <typename Ordering>
  friend auto order_impl(record_view3 l, record_view3 r) -> Ordering;

  friend auto partial_order(record_view3 l, record_view3 r)
    -> std::partial_ordering;
  friend auto weak_order(record_view3 l, record_view3 r) -> std::weak_ordering;

  friend auto partial_order(record_view3 l, const record& r)
    -> std::partial_ordering;
  friend auto weak_order(record_view3 l, const record& r) -> std::weak_ordering;

private:
  record_view3(const arrow::StructArray& array, int64_t index)
    : array_{array}, index_{index} {
    TENZIR_ASSERT(array_.IsValid(index_));
  }

  const arrow::StructArray& array_;
  int64_t index_;
};

class list_view3 {
public:
  class iterator {
  public:
    using difference_type = void;
    using value_type = data_view3;
    using pointer = void;
    using reference = value_type&;
    using iterator_category = std::input_iterator_tag;

    const arrow::ListArray* array;
    int64_t index;
    int64_t offset;

    auto operator*() const -> value_type;

    auto operator++() -> iterator& {
      TENZIR_ASSERT(offset < array->value_offset(index + 1));
      ++offset;
      return *this;
    }

    auto operator==(iterator other) const -> bool {
      TENZIR_ASSERT(array == other.array);
      TENZIR_ASSERT(index == other.index);
      return offset == other.offset;
    }

    auto operator!=(iterator other) const -> bool {
      return ! (*this == other);
    }
  };

  auto at(int64_t index) const -> data_view3;

  static auto from_valid(const arrow::ListArray& array, int64_t index)
    -> list_view3 {
    return list_view3{array, index};
  }

  auto begin() const -> iterator {
    return iterator{&array_, index_, array_.value_offset(index_)};
  }

  auto end() const -> iterator {
    return iterator{&array_, index_, array_.value_offset(index_ + 1)};
  }

  auto size() const -> size_t {
    return static_cast<size_t>(ssize());
  }
  auto ssize() const -> arrow::ListArray::offset_type {
    return array_.value_length(index_);
  }

  friend auto partial_order(list_view3 l, list_view3 r)
    -> std::partial_ordering;
  friend auto weak_order(list_view3 l, list_view3 r) -> std::weak_ordering;

  friend auto partial_order(list_view3 l, const list& r)
    -> std::partial_ordering;
  friend auto weak_order(list_view3 l, const list& r) -> std::weak_ordering;

private:
  list_view3(const arrow::ListArray& array, int64_t index)
    : array_{array}, index_{index} {
    TENZIR_ASSERT(array_.IsValid(index_));
  }

  const arrow::ListArray& array_;
  int64_t index_;
};

using data_view3_base = detail::tl_apply_t<data_view_viewing_types, variant>;

class data_view3 : public data_view3_base {
public:
  data_view3() = default;
  data_view3(const data_view3&) = default;
  data_view3(data_view3&&) = default;

  template <data_view3_type T>
  explicit(false) data_view3(T x) : data_view3_base{std::move(x)} {
  }

  explicit(false) data_view3(const std::string& x)
    : data_view3_base{make_view(x)} {
  }

  explicit(false) data_view3(const char* x)
    : data_view3_base{make_view(std::string_view{x})} {
  }

  template <size_t N>
  explicit(false) data_view3(const char (&x)[N])
    : data_view3_base{make_view(x)} {
  }

  explicit(false) data_view3(const blob& x) : data_view3_base{make_view(x)} {
  }

  explicit(false) data_view3(const secret& x) : data_view3_base{make_view(x)} {
  }
};

inline auto operator==(data_view3 l, data_view3 r) -> bool {
  return partial_order(l, r) == std::partial_ordering::equivalent;
}

inline auto operator==(data_view3 l, const data& r) -> bool {
  return partial_order(l, r) == std::partial_ordering::equivalent;
}

inline auto operator==(const data& l, data_view3 r) -> bool {
  return partial_order(r, l) == std::partial_ordering::equivalent;
}

template <std::derived_from<arrow::Array> T>
auto view_at(const T& x, int64_t i)
  -> std::optional<view3<type_to_data_t<type_from_arrow_t<T>>>> {
  TENZIR_ASSERT(0 <= i);
  TENZIR_ASSERT(i < x.length(),
                "index `{}` is out of range for array of length `{}`", i,
                x.length());
  if (x.IsNull(i)) {
    return std::nullopt;
  }
  if constexpr (std::same_as<T, arrow::NullArray>) {
    TENZIR_UNREACHABLE();
    return std::nullopt;
  } else if constexpr (std::same_as<T, arrow::BooleanArray>) {
    return x.GetView(i);
  } else if constexpr (std::same_as<T, arrow::Int64Array>) {
    return int64_t{x.GetView(i)};
  } else if constexpr (std::same_as<T, arrow::UInt64Array>) {
    return x.GetView(i);
  } else if constexpr (std::same_as<T, arrow::DoubleArray>) {
    return x.GetView(i);
  } else if constexpr (std::same_as<T, arrow::DurationArray>) {
    TENZIR_ASSERT_EXPENSIVE(
      as<type_to_arrow_type_t<duration_type>>(*x.type()).unit()
      == arrow::TimeUnit::NANO);
    return duration{x.GetView(i)};
  } else if constexpr (std::same_as<T, arrow::TimestampArray>) {
    TENZIR_ASSERT_EXPENSIVE(
      as<type_to_arrow_type_t<time_type>>(*x.type()).unit()
      == arrow::TimeUnit::NANO);
    return time{} + duration{x.GetView(i)};
  } else if constexpr (std::same_as<T, arrow::StringArray>) {
    const auto str = x.GetView(i);
    return std::string_view{str.data(), str.size()};
  } else if constexpr (std::same_as<T, ip_type::array_type>) {
    auto storage = x.storage();
    TENZIR_ASSERT_EXPENSIVE(storage->byte_width() == 16);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    const auto* bytes = storage->raw_values() + (i * 16);
    return ip::v6(std::span<const uint8_t, 16>{bytes, 16});
  } else if constexpr (std::same_as<T, subnet_type::array_type>) {
    auto storage = x.storage();
    TENZIR_ASSERT_EXPENSIVE(storage->num_fields() == 2);
    auto network
      = view_at(as<type_to_arrow_array_t<ip_type>>(*storage->field(0)), i);
    TENZIR_ASSERT_EXPENSIVE(network);
    auto length
      = static_cast<const arrow::UInt8Array&>(*storage->field(1)).GetView(i);
    return subnet{*network, length};
  } else if constexpr (std::same_as<T, enumeration_type::array_type>) {
    return detail::narrow_cast<view<type_to_data_t<enumeration_type>>>(
      x.storage()->GetValueIndex(i));
  } else if constexpr (std::same_as<T, arrow::StructArray>) {
    return record_view3::from_valid(x, i);
  } else if constexpr (std::same_as<T, arrow::ListArray>) {
    return list_view3::from_valid(x, i);
  } else if constexpr (std::same_as<T, arrow::BinaryArray>) {
    const auto str = x.GetView(i);
    return blob_view{reinterpret_cast<const std::byte*>(str.data()),
                     str.size()};
  } else if constexpr (std::same_as<T, secret_type::array_type>) {
    auto storage = x.storage();
    TENZIR_ASSERT_EXPENSIVE(storage->num_fields() == 1);
    const auto& bin_array = as<arrow::BinaryArray>(*storage->field(0));
    auto chunk
      = chunk::make(bin_array.value_data())
          ->slice(bin_array.value_offset(i), bin_array.value_length(i));
    auto fbs = detail::secrets::owning_root_fbs_buffer::make(std::move(chunk));
    TENZIR_ASSERT(fbs);
    return secret_view{std::move(*fbs).as_child()};
  } else if constexpr (std::same_as<T, arrow::MapArray>) {
    // TODO: Once we actually get rid of Maps...
    TENZIR_UNREACHABLE();
    return std::nullopt;
  } else {
    static_assert(detail::always_false_v<T>, "unhandled type");
  }
}

template <class DataType>
  requires detail::tl_contains_v<data_view3_owning_types, DataType>
inline auto view_at(const arrow::Array& x, int64_t i) {
  using Tenzir_Type = data_to_type_t<DataType>;
  auto const* arr = dynamic_cast<type_to_arrow_array_t<Tenzir_Type> const*>(&x);
  TENZIR_ASSERT(arr);
  return view_at(*arr, i);
}

template <concrete_type TenzirType>
inline auto view_at(const arrow::Array& x, int64_t i) {
  static_assert(
    detail::tl_contains_v<data_view3_owning_types, type_to_data_t<TenzirType>>);
  auto const* arr = dynamic_cast<type_to_arrow_array_t<TenzirType> const*>(&x);
  TENZIR_ASSERT(arr);
  return view_at(*arr, i);
}

inline auto view_at(const arrow::Array& x, int64_t i) -> data_view3 {
  return match(
    x,
    [&](const auto& x) -> data_view3 {
      if (auto v = view_at(x, i)) {
        return *v;
      }
      return caf::none;
    },
    [&](const arrow::MapArray&) -> data_view3 {
      TENZIR_UNREACHABLE();
    });
}

inline auto list_view3::iterator::operator*() const -> data_view3 {
  TENZIR_ASSERT(offset < array->values()->length());
  TENZIR_ASSERT(offset < array->value_offset(index + 1));
  return view_at(*array->values(), offset);
}

inline auto list_view3::at(int64_t index) const -> data_view3 {
  auto const start = array_.value_offset(index_);
  auto const end = array_.value_offset(index_ + 1);
  TENZIR_ASSERT(index >= 0);
  TENZIR_ASSERT_LT(index, end - start);
  return view_at(*array_.values(), start + index);
};

inline auto record_view3::iterator::operator*() const
  -> std::pair<std::string_view, data_view3> {
  TENZIR_ASSERT(field < array->num_fields());
  return {
    array->type()->field(field)->name(),
    view_at(*array->field(field), index),
  };
}

inline auto values3(const arrow::Array& array) -> generator<data_view3> {
  return match(
    array,
    [&](const auto& x) -> generator<data_view3> {
      for (auto i = int64_t{0}; i < x.length(); ++i) {
        if (auto v = view_at(x, i)) {
          co_yield *v;
        } else {
          co_yield caf::none;
        }
      }
    },
    [&](const arrow::MapArray&) -> generator<data_view3> {
      TENZIR_UNREACHABLE();
    });
}

template <typename T>
  requires(not std::same_as<T, arrow::Array>
           and std::derived_from<T, arrow::Array>)
auto values3(const T& array)
  -> generator<std::optional<view3<type_to_data_t<type_from_arrow_t<T>>>>> {
  for (auto i = int64_t{0}; i < array.length(); ++i) {
    co_yield view_at(array, i);
  }
}

class table_slice;
auto values3(const table_slice& x) -> generator<record_view3>;

auto materialize(record_view3 v) -> record;
auto materialize(list_view3 v) -> list;
auto materialize(data_view3 v) -> data;

template <class T>
auto materialize(view3<T> v) -> T;

class view_wrapper {
public:
  explicit view_wrapper(std::shared_ptr<arrow::Array> array)
    : array_{std::move(array)} {
    TENZIR_ASSERT(array_->length());
  }

  operator data_view3() const {
    return view_at(*array_, 0);
  }

private:
  std::shared_ptr<arrow::Array> array_;
};

auto make_view_wrapper(data_view2 x) -> view_wrapper;

template <class T>
struct view3_to_data {
  using type = T;
};

template <>
struct view3_to_data<std::string_view> {
  using type = std::string;
};

template <>
struct view3_to_data<blob_view> {
  using type = blob;
};

template <>
struct view3_to_data<secret_view> {
  using type = secret;
};

template <>
struct view3_to_data<list_view3> {
  using type = list;
};

template <>
struct view3_to_data<record_view3> {
  using type = record;
};

template <class T>
using view3_to_data_t = typename view3_to_data<T>::type;

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, list_view3 l) noexcept {
  for (auto value : l) {
    hash_append(h, value);
  }
  hash_append(h, l.size());
}

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, record_view3 r) noexcept {
  auto size = size_t{0};
  for (auto [name, value] : r) {
    hash_append(h, name, value);
    ++size;
  }
  hash_append(h, size);
}

template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, data_view3 v) noexcept {
  match(v, [&](auto value) {
    using T = std::decay_t<decltype(value)>;
    using data_type = view3_to_data_t<T>;
    constexpr auto type_tag = [] {
      auto tag = data_to_type_t<data_type>::type_index;
      // Legacy data/data_view hashing tags do not include the internal
      // enriched_type schema slot, so blob/secret must be shifted back by one
      // to stay compatible with heterogeneous lookups against data keys.
      if constexpr (std::same_as<data_type, blob>
                    or std::same_as<data_type, secret>) {
        --tag;
      }
      return tag;
    }();
    hash_append(h, type_tag);
    hash_append(h, value);
  });
}

} // namespace tenzir

namespace std {

template <>
struct hash<::tenzir::list_view3> {
  auto operator()(::tenzir::list_view3 v) const -> std::size_t {
    return std::hash<::tenzir::data_view>{}(v);
  }

  auto operator()(const ::tenzir::list& v) const -> std::size_t {
    return std::hash<::tenzir::data_view>{}(v);
  }
};

template <>
struct hash<::tenzir::record_view3> {
  auto operator()(::tenzir::record_view3 v) const -> std::size_t {
    return std::hash<::tenzir::data_view>{}(v);
  }

  auto operator()(const ::tenzir::record& v) const -> std::size_t {
    return std::hash<::tenzir::data_view>{}(v);
  }
};

template <>
struct hash<tenzir::data_view3> : hash<tenzir::data_view> {};
} // namespace std

namespace fmt {

template <>
struct formatter<tenzir::data_view3> {
  constexpr auto parse(format_parse_context ctx) {
    return ctx.begin();
  }

  auto format(const tenzir::data_view3& value, format_context& ctx) const
    -> format_context::iterator;
};

template <>
struct formatter<tenzir::record_view3> {
  constexpr auto parse(format_parse_context ctx) {
    return ctx.begin();
  }

  auto format(const tenzir::record_view3& value, format_context& ctx) const
    -> format_context::iterator;
};

template <>
struct formatter<tenzir::list_view3> {
  constexpr auto parse(format_parse_context ctx) {
    return ctx.begin();
  }

  auto format(const tenzir::list_view3& value, format_context& ctx) const
    -> format_context::iterator;
};

} // namespace fmt
