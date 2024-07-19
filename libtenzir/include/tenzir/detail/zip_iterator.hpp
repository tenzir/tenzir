//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// C++17 of Zip Iterators, adapted for libtenzir.
//
// Original Author: Dario Pellegrini <pellegrini.dario@gmail.com>
// Originally created: October 2019
// Original License: Creative Commons Zero v1.0 Universal
// Includes code from https://codereview.stackexchange.com/questions/231352/

#pragma once

#include <cstddef>
#include <iterator>
#include <tuple>
#include <utility>

namespace tenzir::detail {

template <typename... T>
class zip_ref {
protected:
  std::tuple<T*...> ptr_;

  template <std::size_t I = 0>
  void copy_assign(const zip_ref& z) {
    *(std::get<I>(ptr_)) = *(std::get<I>(z.ptr_));
    if constexpr (I + 1 < sizeof...(T))
      copy_assign<I + 1>(z);
  }
  template <std::size_t I = 0>
  void val_assign(const std::tuple<T...>& t) {
    *(std::get<I>(ptr_)) = std::get<I>(t);
    if constexpr (I + 1 < sizeof...(T))
      val_assign<I + 1>(t);
  }

public:
  zip_ref() = delete;

  zip_ref(const zip_ref& z) = default;

  zip_ref(zip_ref&& z) = default;

  zip_ref(T* const... p) : ptr_(p...) {
    // nop
  }

  zip_ref& operator=(const zip_ref& z) {
    copy_assign(z);
    return *this;
  }
  zip_ref& operator=(const std::tuple<T...>& val) {
    val_assign(val);
    return *this;
  }

  [[nodiscard]] std::tuple<T...> val() const {
    return std::apply([](auto&&... args) { return std::tuple((*args)...); },
                      ptr_);
  }
  operator std::tuple<T...>() const {
    return val();
  }

  template <std::size_t I = 0>
  void swap_data(const zip_ref& o) const {
    std::swap(*std::get<I>(ptr_), *std::get<I>(o.ptr_));
    if constexpr (I + 1 < sizeof...(T))
      swap_data<I + 1>(o);
  }

  template <std::size_t N = 0>
  decltype(auto) get() {
    return *std::get<N>(ptr_);
  }

  template <std::size_t N = 0>
  [[nodiscard]] decltype(auto) get() const {
    return *std::get<N>(ptr_);
  }

  bool operator==(const zip_ref& o) const {
    return val() == o.val();
  }

  inline friend bool operator==(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() == t;
  }

  inline friend bool operator==(const std ::tuple<T...>& t, const zip_ref& r) {
    return t == r.val();
  }

  bool operator<=(const zip_ref& o) const {
    return val() <= o.val();
  }

  inline friend bool operator<=(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() <= t;
  }

  inline friend bool operator<=(const std ::tuple<T...>& t, const zip_ref& r) {
    return t <= r.val();
  }

  bool operator>=(const zip_ref& o) const {
    return val() >= o.val();
  }

  inline friend bool operator>=(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() >= t;
  }

  inline friend bool operator>=(const std ::tuple<T...>& t, const zip_ref& r) {
    return t >= r.val();
  }

  bool operator!=(const zip_ref& o) const {
    return val() != o.val();
  }

  inline friend bool operator!=(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() != t;
  }

  inline friend bool operator!=(const std ::tuple<T...>& t, const zip_ref& r) {
    return t != r.val();
  }

  bool operator<(const zip_ref& o) const {
    return val() < o.val();
  }

  inline friend bool operator<(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() < t;
  }

  inline friend bool operator<(const std ::tuple<T...>& t, const zip_ref& r) {
    return t < r.val();
  }

  bool operator>(const zip_ref& o) const {
    return val() > o.val();
  }

  inline friend bool operator>(const zip_ref& r, const std ::tuple<T...>& t) {
    return r.val() > t;
  }

  inline friend bool operator>(const std ::tuple<T...>& t, const zip_ref& r) {
    return t > r.val();
  }
};

} // namespace tenzir::detail

namespace std {

template <std::size_t N, typename... T>
struct tuple_element<N, tenzir::detail::zip_ref<T...>> {
  using type = decltype(std::get<N>(
    std::declval<tenzir::detail::zip_ref<T...>>().val()));
};

template <typename... T>
struct tuple_size<tenzir::detail::zip_ref<T...>>
  : public std::integral_constant<std::size_t, sizeof...(T)> {};

template <std::size_t N, typename... T>
decltype(auto) get(tenzir::detail::zip_ref<T...>& r) {
  return r.template get<N>();
}
template <std::size_t N, typename... T>
decltype(auto) get(const tenzir::detail::zip_ref<T...>& r) {
  return r.template get<N>();
}

} // namespace std

namespace tenzir::detail {

template <typename... Iterator>
class zip_iterator {
  std::tuple<Iterator...> it_;

  template <std::size_t I = 0>
  [[nodiscard]] bool one_is_equal(const zip_iterator& rhs) const {
    if (std::get<I>(it_) == std::get<I>(rhs.it_))
      return true;
    if constexpr (I + 1 < sizeof...(Iterator))
      return one_is_equal<I + 1>(rhs);
    return false;
  }

  template <std::size_t I = 0>
  [[nodiscard]] bool none_is_equal(const zip_iterator& rhs) const {
    if (std::get<I>(it_) == std::get<I>(rhs.it_))
      return false;
    if constexpr (I + 1 < sizeof...(Iterator))
      return none_is_equal<I + 1>(rhs);
    return true;
  }

public:
  using iterator_category = std::common_type_t<
    typename std::iterator_traits<Iterator>::iterator_category...>;

  using difference_type = std::common_type_t<
    typename std::iterator_traits<Iterator>::difference_type...>;

  using value_type
    = std::tuple<typename std::iterator_traits<Iterator>::value_type...>;

  using pointer
    = std::tuple<typename std::iterator_traits<Iterator>::pointer...>;

  using reference = zip_ref<std::remove_reference_t<
    typename std::iterator_traits<Iterator>::reference>...>;

  zip_iterator() = default;

  zip_iterator(const zip_iterator& rhs) = default;

  zip_iterator(zip_iterator&& rhs) = default;

  zip_iterator(const Iterator&... rhs) : it_(rhs...) {
    // nop
  }

  zip_iterator& operator=(const zip_iterator& rhs) = default;

  zip_iterator& operator=(zip_iterator&& rhs) = default;

  zip_iterator& operator+=(const difference_type d) {
    std::apply([&d](auto&&... args) { ((std::advance(args, d)), ...); }, it_);
    return *this;
  }
  zip_iterator& operator-=(const difference_type d) {
    return operator+=(-d);
  }

  reference operator*() const {
    return std::apply([](auto&&... args) { return reference(&(*(args))...); },
                      it_);
  }

  pointer operator->() const {
    return std::apply([](auto&&... args) { return pointer(&(*(args))...); },
                      it_);
  }

  reference operator[](difference_type rhs) const {
    return *(operator+(rhs));
  }

  zip_iterator& operator++() {
    return operator+=(1);
  }
  zip_iterator& operator--() {
    return operator+=(-1);
  }
  zip_iterator operator++(int) {
    zip_iterator tmp(*this);
    operator++();
    return tmp;
  }
  zip_iterator operator--(int) {
    zip_iterator tmp(*this);
    operator--();
    return tmp;
  }

  difference_type operator-(const zip_iterator& rhs) const {
    return std::get<0>(it_) - std::get<0>(rhs.it_);
  }

  zip_iterator operator+(const difference_type d) const {
    zip_iterator tmp(*this);
    tmp += d;
    return tmp;
  }
  zip_iterator operator-(const difference_type d) const {
    zip_iterator tmp(*this);
    tmp -= d;
    return tmp;
  }

  inline friend zip_iterator
  operator+(const difference_type d, const zip_iterator& z) {
    return z + d;
  }

  inline friend zip_iterator
  operator-(const difference_type d, const zip_iterator& z) {
    return z - d;
  }

  bool operator==(const zip_iterator& rhs) const {
    return one_is_equal(rhs);
  }

  bool operator!=(const zip_iterator& rhs) const {
    return none_is_equal(rhs);
  }

  bool operator<=(const zip_iterator& rhs) const {
    return it_ <= rhs.it_;
  }

  bool operator>=(const zip_iterator& rhs) const {
    return it_ >= rhs.it_;
  }

  bool operator<(const zip_iterator& rhs) const {
    return it_ < rhs.it_;
  }

  bool operator>(const zip_iterator& rhs) const {
    return it_ > rhs.it_;
  }
};

template <typename... Container>
class zip {
  std::tuple<Container&...> zip_;

public:
  zip() = delete;

  zip(const zip& z) = default;

  zip(zip&& z) = default;

  zip(Container&... z) : zip_(z...) {
    // nop
  }

  auto begin() {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.begin())...); }, zip_);
  }

  [[nodiscard]] auto cbegin() const {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.cbegin())...); }, zip_);
  }

  [[nodiscard]] auto begin() const {
    return this->cbegin();
  }

  auto end() {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.end())...); }, zip_);
  }

  [[nodiscard]] auto cend() const {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.cend())...); }, zip_);
  }

  [[nodiscard]] auto end() const {
    return this->cend();
  }

  auto rbegin() {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.rbegin())...); }, zip_);
  }

  [[nodiscard]] auto crbegin() const {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.crbegin())...); }, zip_);
  }

  [[nodiscard]] auto rbegin() const {
    return this->crbegin();
  }

  auto rend() {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.rend())...); }, zip_);
  }

  [[nodiscard]] auto crend() const {
    return std ::apply(
      [](auto&&... args) { return zip_iterator((args.crend())...); }, zip_);
  }

  [[nodiscard]] auto rend() const {
    return this->crend();
  }
};

template <typename... T>
void swap(const zip_ref<T...>& lhs, const zip_ref<T...>& rhs) {
  lhs.swap_data(rhs);
}

template <class T, class... Ts>
auto zip_equal(T& x, Ts&... xs) -> detail::zip<T, Ts...> {
  auto size = x.size();
  auto match = ((xs.size() == size) && ...);
  TENZIR_ASSERT(match);
  return detail::zip{x, xs...};
}

} // namespace tenzir::detail
