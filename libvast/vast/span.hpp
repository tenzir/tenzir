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

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/Microsoft/GSL
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       GSL/include/gsl/span
// - Author:     Microsoft
// - Copyright:  (c) 2015 Microsoft Corporation. All rights reserved.
// - License:    MIT

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include "vast/byte.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/narrow.hpp"

namespace vast {

// [views.constants], constants
constexpr const std::ptrdiff_t dynamic_extent = -1;

template <class ElementType, std::ptrdiff_t Extent = dynamic_extent>
class span;

namespace detail {

template <class T>
struct is_span_oracle : std::false_type {};

template <class ElementType, std::ptrdiff_t Extent>
struct is_span_oracle<span<ElementType, Extent>> : std::true_type {};

template <class T>
constexpr auto is_span_oracle_v = is_span_oracle<T>::value;

template <class T>
struct is_span : public is_span_oracle<std::remove_cv_t<T>> {};

template <class T>
constexpr auto is_span_v = is_span<T>::value;

template <class T>
struct is_std_array_oracle : std::false_type {};

template <class ElementType, std::size_t Extent>
struct is_std_array_oracle<std::array<ElementType, Extent>> : std::true_type {};

template <class T>
struct is_std_array : public is_std_array_oracle<std::remove_cv_t<T>> {};

template <class T>
constexpr auto is_std_array_v = is_std_array<T>::value;

template <std::ptrdiff_t From, std::ptrdiff_t To>
struct is_allowed_extent_conversion
  : public std::integral_constant<bool, From == To || From == dynamic_extent
                                          || To == dynamic_extent> {};

template <std::ptrdiff_t From, std::ptrdiff_t To>
constexpr auto is_allowed_extent_conversion_v
  = is_allowed_extent_conversion<From, To>::value;

template <class From, class To>
struct is_allowed_element_type_conversion
  : public std::integral_constant<bool,
                                  std::is_convertible_v<From (*)[], To (*)[]>> {
};

template <class Span, bool IsConst>
class span_iterator {
  using element_type_ = typename Span::element_type;

public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = std::remove_cv_t<element_type_>;
  using difference_type = typename Span::index_type;

  using reference
    = std::conditional_t<IsConst, const element_type_, element_type_>&;
  using pointer = std::add_pointer_t<reference>;

  span_iterator() = default;

  constexpr span_iterator(const Span* span,
                          typename Span::index_type idx) noexcept
    : span_(span), index_(idx) {
  }

  friend span_iterator<Span, true>;

  template <bool B, std::enable_if_t<!B && IsConst>* = nullptr>
  constexpr span_iterator(const span_iterator<Span, B>& other) noexcept
    : span_iterator(other.span_, other.index_) {
  }

  constexpr reference operator*() const {
    //VAST_ASSERT(index_ != span_->size());
    return *(span_->data() + index_);
  }

  constexpr pointer operator->() const {
    //VAST_ASSERT(index_ != span_->size());
    return span_->data() + index_;
  }

  constexpr span_iterator& operator++() {
    //VAST_ASSERT(0 <= index_ && index_ != span_->size());
    ++index_;
    return *this;
  }

  constexpr span_iterator operator++(int) {
    auto ret = *this;
    ++(*this);
    return ret;
  }

  constexpr span_iterator& operator--() {
    //VAST_ASSERT(index_ != 0 && index_ <= span_->size());
    --index_;
    return *this;
  }

  constexpr span_iterator operator--(int) {
    auto ret = *this;
    --(*this);
    return ret;
  }

  constexpr span_iterator operator+(difference_type n) const {
    auto ret = *this;
    return ret += n;
  }

  constexpr span_iterator& operator+=(difference_type n) {
    //VAST_ASSERT((index_ + n) >= 0 && (index_ + n) <= span_->size());
    index_ += n;
    return *this;
  }

  constexpr span_iterator operator-(difference_type n) const {
    auto ret = *this;
    return ret -= n;
  }

  constexpr span_iterator& operator-=(difference_type n) {
    return *this += -n;
  }

  constexpr difference_type operator-(span_iterator rhs) const {
    //VAST_ASSERT(span_ == rhs.span_);
    return index_ - rhs.index_;
  }

  constexpr reference operator[](difference_type n) const {
    return *(*this + n);
  }

  constexpr friend bool operator==(span_iterator lhs,
                                   span_iterator rhs) noexcept {
    return lhs.span_ == rhs.span_ && lhs.index_ == rhs.index_;
  }

  constexpr friend bool operator!=(span_iterator lhs,
                                   span_iterator rhs) noexcept {
    return !(lhs == rhs);
  }

  constexpr friend bool operator<(span_iterator lhs,
                                  span_iterator rhs) noexcept {
    return lhs.index_ < rhs.index_;
  }

  constexpr friend bool operator<=(span_iterator lhs,
                                   span_iterator rhs) noexcept {
    return !(rhs < lhs);
  }

  constexpr friend bool operator>(span_iterator lhs,
                                  span_iterator rhs) noexcept {
    return rhs < lhs;
  }

  constexpr friend bool operator>=(span_iterator lhs,
                                   span_iterator rhs) noexcept {
    return !(rhs > lhs);
  }

protected:
  const Span* span_ = nullptr;
  std::ptrdiff_t index_ = 0;
};

template <class Span, bool IsConst>
constexpr span_iterator<Span, IsConst>
operator+(typename span_iterator<Span, IsConst>::difference_type n,
          span_iterator<Span, IsConst> rhs) {
  return rhs + n;
}

template <class Span, bool IsConst>
constexpr span_iterator<Span, IsConst>
operator-(typename span_iterator<Span, IsConst>::difference_type n,
          span_iterator<Span, IsConst> rhs) {
  return rhs - n;
}

template <std::ptrdiff_t Ext>
class extent_type {
public:
  using index_type = std::ptrdiff_t;

  static_assert(Ext >= 0, "A fixed-size span must be >= 0 in size.");

  constexpr extent_type() noexcept {
  }

  template <index_type Other>
  constexpr extent_type(extent_type<Other> /* ext */) {
    static_assert(
      Other == Ext || Other == dynamic_extent,
      "Mismatch between fixed-size extent and size of initializing data.");
    //VAST_ASSERT(ext.size() == Ext);
  }

  constexpr extent_type(index_type /* size */) {
    //VAST_ASSERT(size == Ext);
  }

  constexpr index_type size() const noexcept {
    return Ext;
  }
};

template <>
class extent_type<dynamic_extent> {
public:
  using index_type = std::ptrdiff_t;

  template <index_type Other>
  explicit constexpr extent_type(extent_type<Other> ext) : size_(ext.size()) {
  }

  explicit constexpr extent_type(index_type size) : size_(size) {
    //VAST_ASSERT(size >= 0);
  }

  constexpr index_type size() const noexcept {
    return size_;
  }

private:
  index_type size_;
};

template <class ElementType, std::ptrdiff_t Extent, std::ptrdiff_t Offset,
          std::ptrdiff_t Count>
struct calculate_subspan_type {
  using type = span<ElementType,
                    Count != dynamic_extent ?
                      Count :
                      (Extent != dynamic_extent ? Extent - Offset : Extent)>;
};

} // namespace detail

// [span], class template span
template <class ElementType, std::ptrdiff_t Extent>
class span {
public:
  // constants and types
  using element_type = ElementType;
  using value_type = std::remove_cv_t<ElementType>;
  using index_type = std::ptrdiff_t;
  using pointer = element_type*;
  using reference = element_type&;

  using iterator = detail::span_iterator<span<ElementType, Extent>, false>;
  using const_iterator = detail::span_iterator<span<ElementType, Extent>, true>;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  using size_type = index_type;

  static constexpr index_type extent{Extent};

  // [span.cons], span constructors, copy, assignment, and destructor
  template <bool Dependent = false,
            // "Dependent" is needed to make "std::enable_if_t<Dependent ||
            // Extent <= 0>" SFINAE, since "std::enable_if_t<Extent <= 0>" is
            // ill-formed when Extent is greater than 0.
            class = std::enable_if_t<(Dependent || Extent <= 0)>>
  constexpr span() noexcept : storage_(nullptr, detail::extent_type<0>()) {
  }

  constexpr span(pointer ptr, index_type count) : storage_(ptr, count) {
  }

  constexpr span(pointer firstElem, pointer lastElem)
    : storage_(firstElem, std::distance(firstElem, lastElem)) {
  }

  template <std::size_t N>
  constexpr span(element_type (&arr)[N]) noexcept
    : storage_(KnownNotNull{&arr[0]}, detail::extent_type<N>()) {
  }

  template <std::size_t N,
            class ArrayElementType = std::remove_const_t<element_type>>
  constexpr span(std::array<ArrayElementType, N>& arr) noexcept
    : storage_(std::data(arr), detail::extent_type<N>()) {
  }

  template <std::size_t N>
  constexpr span(
    const std::array<std::remove_const_t<element_type>, N>& arr) noexcept
    : storage_(std::data(arr), detail::extent_type<N>()) {
  }

  // NB: the SFINAE here uses .data() as a incomplete/imperfect proxy for the
  // requirement on Container to be a contiguous sequence container.
  template <
    class Container,
    class = std::enable_if_t<
      !detail::is_span_v<Container>
      && !detail::is_std_array_v<Container>
      && std::is_convertible_v<typename Container::pointer, pointer>
      && std::is_convertible_v<
           typename Container::pointer,
           decltype(std::declval<Container>().data())>>>
  constexpr span(Container& cont)
    : span(cont.data(), detail::narrow<index_type>(cont.size())) {
  }

  template <
    class Container,
    class = std::enable_if_t<
      std::is_const_v<element_type> && !detail::is_span_v<Container>
      && std::is_convertible_v<typename Container::pointer, pointer>
      && std::is_convertible_v<
           typename Container::pointer,
           decltype(std::declval<Container>().data())>>>
  constexpr span(const Container& cont)
    : span(cont.data(), detail::narrow<index_type>(cont.size())) {
  }

  constexpr span(const span& other) noexcept = default;

  template <
    class OtherElementType, std::ptrdiff_t OtherExtent,
    class = std::enable_if_t<
      detail::is_allowed_extent_conversion_v<
        OtherExtent,
        Extent> 
      && detail::is_allowed_element_type_conversion<OtherElementType, element_type>::value
    >
  >
  constexpr span(const span<OtherElementType, OtherExtent>& other)
    : storage_(other.data(), detail::extent_type<OtherExtent>(other.size())) {
  }

  ~span() noexcept = default;

  constexpr span& operator=(const span& other) noexcept = default;

  // [span.sub], span subviews
  template <std::ptrdiff_t Count>
  constexpr span<element_type, Count> first() const {
    //VAST_ASSERT(Count >= 0 && Count <= size());
    return {data(), Count};
  }

  template <std::ptrdiff_t Count>
  constexpr span<element_type, Count> last() const {
    //VAST_ASSERT(Count >= 0 && size() - Count >= 0);
    return {data() + (size() - Count), Count};
  }

  template <std::ptrdiff_t Offset, std::ptrdiff_t Count = dynamic_extent>
  constexpr auto subspan() const ->
    typename detail::calculate_subspan_type<ElementType, Extent, Offset, Count>::type {
    //VAST_ASSERT(
    //(Offset >= 0 && size() - Offset >= 0)
    //&& (Count == dynamic_extent || (Count >= 0 && Offset + Count <= size())));

    return {data() + Offset, Count == dynamic_extent ? size() - Offset : Count};
  }

  constexpr span<element_type, dynamic_extent> first(index_type count) const {
    //VAST_ASSERT(count >= 0 && count <= size());
    return {data(), count};
  }

  constexpr span<element_type, dynamic_extent> last(index_type count) const {
    return make_subspan(size() - count, dynamic_extent,
                        subspan_selector<Extent>{});
  }

  constexpr span<element_type, dynamic_extent>
  subspan(index_type offset, index_type count = dynamic_extent) const {
    return make_subspan(offset, count, subspan_selector<Extent>{});
  }

  // [span.obs], span observers
  constexpr index_type size() const noexcept {
    return storage_.size();
  }
  constexpr index_type size_bytes() const noexcept {
    return size() * detail::narrow_cast<index_type>(sizeof(element_type));
  }
  constexpr bool empty() const noexcept {
    return size() == 0;
  }

  // [span.elem], span element access
  constexpr reference operator[](index_type idx) const {
    //VAST_ASSERT(idx >= 0 && idx < storage_.size());
    return data()[idx];
  }

  constexpr reference at(index_type idx) const {
    return this->operator[](idx);
  }
  constexpr reference operator()(index_type idx) const {
    return this->operator[](idx);
  }
  constexpr pointer data() const noexcept {
    return storage_.data();
  }

  // [span.iter], span iterator support
  constexpr iterator begin() const noexcept {
    return {this, 0};
  }
  constexpr iterator end() const noexcept {
    return {this, size()};
  }

  constexpr const_iterator cbegin() const noexcept {
    return {this, 0};
  }
  constexpr const_iterator cend() const noexcept {
    return {this, size()};
  }

  constexpr reverse_iterator rbegin() const noexcept {
    return reverse_iterator{end()};
  }
  constexpr reverse_iterator rend() const noexcept {
    return reverse_iterator{begin()};
  }

  constexpr const_reverse_iterator crbegin() const noexcept {
    return const_reverse_iterator{cend()};
  }
  constexpr const_reverse_iterator crend() const noexcept {
    return const_reverse_iterator{cbegin()};
  }

private:
  // Needed to remove unnecessary null check in subspans
  struct KnownNotNull {
    pointer p;
  };

  // this implementation detail class lets us take advantage of the
  // empty base class optimization to pay for only storage of a single
  // pointer in the case of fixed-size spans
  template <class ExtentType>
  class storage_type : public ExtentType {
  public:
    // KnownNotNull parameter is needed to remove unnecessary null check
    // in subspans and constructors from arrays
    template <class OtherExtentType>
    constexpr storage_type(KnownNotNull data, OtherExtentType ext)
      : ExtentType(ext), data_(data.p) {
      //VAST_ASSERT(ExtentType::size() >= 0);
    }

    template <class OtherExtentType>
    constexpr storage_type(pointer data, OtherExtentType ext)
      : ExtentType(ext), data_(data) {
      //VAST_ASSERT(ExtentType::size() >= 0);
      //VAST_ASSERT(data || ExtentType::size() == 0);
    }

    constexpr pointer data() const noexcept {
      return data_;
    }

  private:
    pointer data_;
  };

  storage_type<detail::extent_type<Extent>> storage_;

  // The rest is needed to remove unnecessary null check
  // in subspans and constructors from arrays
  constexpr span(KnownNotNull ptr, index_type count) : storage_(ptr, count) {
    // nop
  }

  template <std::ptrdiff_t CallerExtent>
  class subspan_selector {};

  template <std::ptrdiff_t CallerExtent>
  span<element_type, dynamic_extent>
  make_subspan(index_type offset, index_type count,
               subspan_selector<CallerExtent>) const {
    span<element_type, dynamic_extent> tmp(*this);
    return tmp.subspan(offset, count);
  }

  span<element_type, dynamic_extent>
  make_subspan(index_type offset, index_type count,
               subspan_selector<dynamic_extent>) const {
    VAST_ASSERT(offset >= 0 && size() - offset >= 0);
    if (count == dynamic_extent)
      return {KnownNotNull{data() + offset}, size() - offset};
    VAST_ASSERT(count >= 0 && size() - offset >= count);
    return {KnownNotNull{data() + offset}, count};
  }
};

// [span.comparison], span comparison operators
template <class ElementType, std::ptrdiff_t FirstExtent,
          std::ptrdiff_t SecondExtent>
constexpr bool operator==(span<ElementType, FirstExtent> l,
                          span<ElementType, SecondExtent> r) {
  return std::equal(l.begin(), l.end(), r.begin(), r.end());
}

template <class ElementType, std::ptrdiff_t Extent>
constexpr bool operator!=(span<ElementType, Extent> l,
                          span<ElementType, Extent> r) {
  return !(l == r);
}

template <class ElementType, std::ptrdiff_t Extent>
constexpr bool operator<(span<ElementType, Extent> l,
                         span<ElementType, Extent> r) {
  return std::lexicographical_compare(l.begin(), l.end(), r.begin(), r.end());
}

template <class ElementType, std::ptrdiff_t Extent>
constexpr bool operator<=(span<ElementType, Extent> l,
                          span<ElementType, Extent> r) {
  return !(l > r);
}

template <class ElementType, std::ptrdiff_t Extent>
constexpr bool operator>(span<ElementType, Extent> l,
                         span<ElementType, Extent> r) {
  return r < l;
}

template <class ElementType, std::ptrdiff_t Extent>
constexpr bool operator>=(span<ElementType, Extent> l,
                          span<ElementType, Extent> r) {
  return !(l < r);
}

// if we only supported compilers with good constexpr support then
// this pair of classes could collapse down to a constexpr function

// we should use a narrow_cast<> to go to std::size_t, but older compilers may
// not see it as constexpr and so will fail compilation of the template
template <class ElementType, std::ptrdiff_t Extent>
struct calculate_byte_size
  : std::integral_constant<std::ptrdiff_t,
                           static_cast<std::ptrdiff_t>(
                             sizeof(ElementType)
                             * static_cast<std::size_t>(Extent))> {};

template <class ElementType>
struct calculate_byte_size<ElementType, dynamic_extent>
  : std::integral_constant<std::ptrdiff_t, dynamic_extent> {};

// [span.objectrep], views of object representation
template <class ElementType, std::ptrdiff_t Extent>
span<const byte, calculate_byte_size<ElementType, Extent>::value>
as_bytes(span<ElementType, Extent> s) noexcept {
  return {reinterpret_cast<const byte*>(s.data()), s.size_bytes()};
}

template <class ElementType, std::ptrdiff_t Extent,
          class = std::enable_if_t<!std::is_const_v<ElementType>>>
span<byte, calculate_byte_size<ElementType, Extent>::value>
as_writeable_bytes(span<ElementType, Extent> s) noexcept {
  return {reinterpret_cast<byte*>(s.data()), s.size_bytes()};
}

// -- non-standard utilitiy functions ------------------------------------------

/// Constructs a byte span from an arbitrary pointer type and size.
/// @tparam T The element type of the span.
/// @tparam Size The size type casted into the span's `index_type`.
/// @param data A pointer to data.
/// @param size The length of the byte sequence at *data*.
/// @returns A `span<byte>` or `span<const byte>` over *data* of length *size*.
///          Constness is propagated through `T`.
template <class T, class Size>
auto make_byte_span(T* data, Size size) {
  using byte_type = std::conditional_t<
    std::is_const_v<std::remove_pointer_t<T>>,
    const byte,
    byte
  >;
  auto ptr = reinterpret_cast<byte_type*>(data);
  using span_type = span<byte_type>;
  using index_type = typename span_type::index_type;
  return span_type{ptr, detail::narrow_cast<index_type>(size)};
}

/// @relates make_byte_span
template <class Container>
auto make_byte_span(Container&& xs) {
  return make_byte_span(std::data(xs), std::size(xs));
}

/// @relates make_byte_span
template <class T, class Size>
auto make_const_byte_span(T* data, Size size) {
  return make_byte_span(const_cast<const T*>(data), size);
}

/// @relates make_byte_span
template <class Container>
auto make_const_byte_span(Container&& xs) {
  return make_const_byte_span(std::data(xs), std::size(xs));
}

} // namespace vast
