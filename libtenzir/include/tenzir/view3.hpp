//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/detail/enumerate.hpp"
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

using data_view_types
  = detail::tl_map_t<detail::tl_filter_t<data::types, derive_data_view>,
                     view_trait3>;

using data_view3 = detail::tl_apply_t<data_view_types, variant>;

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
auto partial_order(const data_view3 l, const data_view3 r)
  -> std::partial_ordering;
/// Establishes a weak ordering, suitable for usage with sorting algorithms. See
/// above for details.
auto weak_order(const data_view3 l, const data_view3 r) -> std::weak_ordering;

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
      return !(*this == other);
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
  friend auto order_impl(const record_view3 l, const record_view3 r)
    -> Ordering;

  friend auto partial_order(const record_view3 l, const record_view3 r)
    -> std::partial_ordering;
  friend auto weak_order(const record_view3 l, const record_view3 r)
    -> std::weak_ordering;

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
      return !(*this == other);
    }
  };

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

  friend auto partial_order(const list_view3 l, const list_view3 r)
    -> std::partial_ordering;
  friend auto weak_order(const list_view3 l, const list_view3 r)
    -> std::weak_ordering;

private:
  list_view3(const arrow::ListArray& array, int64_t index)
    : array_{array}, index_{index} {
    TENZIR_ASSERT(array_.IsValid(index_));
  }

  const arrow::ListArray& array_;
  int64_t index_;
};

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
  if constexpr (std::same_as<T, arrow::StructArray>) {
    return record_view3::from_valid(x, i);
  } else if constexpr (std::same_as<T, arrow::ListArray>) {
    return list_view3::from_valid(x, i);
  } else if constexpr (std::same_as<T, arrow::MapArray>) {
    // TODO: Once we actually get rid of Maps...
    TENZIR_UNREACHABLE();
    return std::nullopt;
  } else {
    return value_at(type_from_arrow_t<T>{}, x, i);
  }
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

} // namespace tenzir

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
