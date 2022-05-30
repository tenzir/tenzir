//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// cppcoro, adapted for libvast.
//
// Original Author: Lewis Baker
// Original License: MIT
// Original Revision: a87e97fe5b6091ca9f6de4637736b8e0d8b109cf
// Includes code from https://github.com/lewissbaker/cppcoro

#pragma once

#include "vast/fwd.hpp"

#include <exception>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

#if __has_include(<coroutine>)

#  include <coroutine>
namespace stdcoro = std;

#else

#  include <experimental/coroutine>
namespace stdcoro = std::experimental;

#endif

namespace vast::detail {

/// A generator represents a coroutine type that produces a sequence of values
/// of type, T, where values are produced lazily and synchronously.
///
/// The coroutine body is able to yield values of type T using the co_yield
/// keyword. Note, however, that the coroutine body is not able to use the
/// co_await keyword; values must be produced synchronously.
///
/// When a coroutine function returning a generator<T> is called the coroutine
/// is created initially suspended. Execution of the coroutine enters the
/// coroutine body when the generator<T>::begin() method is called and continues
/// until either the first co_yield statement is reached or the coroutine runs
/// to completion. If the returned iterator is not equal to the end() iterator
/// then dereferencing the iterator will return a reference to the value passed
/// to the co_yield statement. Calling operator++() on the iterator will resume
/// execution of the coroutine and continue until either the next co_yield point
/// is reached or the coroutine runs to completion(). Any unhandled exceptions
/// thrown by the coroutine will propagate out of the begin() or operator++()
/// calls to the caller.
template <typename T>
class generator;

namespace internal {

template <typename T>
class generator_promise {
public:
  using value_type = std::remove_reference_t<T>;
  using reference_type = T&&;
  using pointer_type = value_type*;

  generator_promise() = default;

  generator<T> get_return_object() noexcept;

  constexpr stdcoro::suspend_always initial_suspend() const noexcept {
    return {};
  }
  constexpr stdcoro::suspend_always final_suspend() const noexcept {
    return {};
  }

  // This overload copies v to a temporary, then yields a pointer to that.
  // This allows passing an lvalue to co_yield for a generator<NotReference>.
  // Looks crazy, but taken from the reference implementation in P2529R0.
  auto yield_value(const T& v) requires(
    !std::is_reference_v<T> && std::copy_constructible<T>) {
    struct Owner : stdcoro::suspend_always {
      Owner(const T& val, pointer_type& out) : v(val) {
        out = &v;
      }
      Owner(Owner&&) = delete;
      T v;
    };
    return Owner(v, m_value);
  }

  stdcoro::suspend_always
  yield_value(std::remove_reference_t<T>&& value) noexcept {
    m_value = std::addressof(value);
    return {};
  }

  void unhandled_exception() {
    m_exception = std::current_exception();
  }

  void return_void() {
  }

  reference_type value() const noexcept {
    return static_cast<reference_type>(*m_value);
  }

  // Don't allow any use of 'co_await' inside the generator coroutine.
  template <typename U>
  stdcoro::suspend_never await_transform(U&& value) = delete;

  void rethrow_if_exception() {
    if (m_exception) {
      std::rethrow_exception(m_exception);
    }
  }

private:
  pointer_type m_value;
  std::exception_ptr m_exception;
};

struct generator_sentinel {};

template <typename T>
class generator_iterator {
  using coroutine_handle = stdcoro::coroutine_handle<generator_promise<T>>;

public:
  using iterator_category = std::input_iterator_tag;
  // What type should we use for counting elements of a potentially infinite
  // sequence?
  using difference_type = std::ptrdiff_t;
  using value_type = typename generator_promise<T>::value_type;
  using reference = typename generator_promise<T>::reference_type;
  using pointer = typename generator_promise<T>::pointer_type;

  // Iterator needs to be default-constructible to satisfy the Range concept.
  generator_iterator() noexcept : m_coroutine(nullptr) {
  }

  explicit generator_iterator(coroutine_handle coroutine) noexcept
    : m_coroutine(coroutine) {
  }

  friend bool
  operator==(const generator_iterator& it, generator_sentinel) noexcept {
    return !it.m_coroutine || it.m_coroutine.done();
  }

  friend bool
  operator!=(const generator_iterator& it, generator_sentinel s) noexcept {
    return !(it == s);
  }

  friend bool
  operator==(generator_sentinel s, const generator_iterator& it) noexcept {
    return (it == s);
  }

  friend bool
  operator!=(generator_sentinel s, const generator_iterator& it) noexcept {
    return it != s;
  }

  generator_iterator& operator++() {
    m_coroutine.resume();
    if (m_coroutine.done()) {
      m_coroutine.promise().rethrow_if_exception();
    }

    return *this;
  }

  // Need to provide post-increment operator to implement the 'Range' concept.
  void operator++(int) {
    (void)operator++();
  }

  reference operator*() const noexcept {
    return m_coroutine.promise().value();
  }

private:
  coroutine_handle m_coroutine;
};

} // namespace internal

template <typename T>
class [[nodiscard]] generator {
public:
  using promise_type = internal::generator_promise<T>;
  using iterator = internal::generator_iterator<T>;

  generator() noexcept : m_coroutine(nullptr) {
  }

  generator(generator&& other) noexcept : m_coroutine(other.m_coroutine) {
    other.m_coroutine = nullptr;
  }

  generator(const generator& other) = delete;

  ~generator() {
    if (m_coroutine)
      m_coroutine.destroy();
  }

  generator& operator=(generator other) noexcept {
    swap(other);
    return *this;
  }

  generator& operator=(generator&& other) noexcept {
    if (m_coroutine)
      m_coroutine.destroy();
    m_coroutine = other.m_coroutine;
    other.m_coroutine = nullptr;
    return *this;
  }

  iterator begin() {
    if (m_coroutine) {
      m_coroutine.resume();
      if (m_coroutine.done()) {
        m_coroutine.promise().rethrow_if_exception();
      }
    }

    return iterator{m_coroutine};
  }

  internal::generator_sentinel end() noexcept {
    return internal::generator_sentinel{};
  }

  void swap(generator& other) noexcept {
    std::swap(m_coroutine, other.m_coroutine);
  }

private:
  friend class internal::generator_promise<T>;

  explicit generator(stdcoro::coroutine_handle<promise_type> coroutine) noexcept
    : m_coroutine(coroutine) {
  }

  stdcoro::coroutine_handle<promise_type> m_coroutine;
};

template <typename T>
void swap(generator<T>& a, generator<T>& b) {
  a.swap(b);
}

namespace internal {

template <typename T>
generator<T> generator_promise<T>::get_return_object() noexcept {
  using coroutine_handle = stdcoro::coroutine_handle<generator_promise<T>>;
  return generator<T>{coroutine_handle::from_promise(*this)};
}

} // namespace internal

/// A utility function to collect all results produced by a `generator<T>` into
/// a suitable container.
///
/// Example:
///     auto g = vast::detail::generator<string_view>{};
///     auto v = vast::detail::collect<std::vector<std::string>>(g);
template <class Container, class T>
  requires requires(Container c, T& t) {
    c.reserve(size_t{});
    c.emplace(c.end(), t);
  }
Container collect(vast::detail::generator<T> g, size_t size_hint = 0) {
  Container result = {};
  if (size_hint)
    result.reserve(result.size() + size_hint);
  for (auto&& x : std::move(g))
    result.emplace(result.end(), std::move(x));
  return result;
}

/// A utility function to collect all results produced by a `generator<T>` into
/// a `std::vector<T>`.
///
/// Example:
///     auto g = vast::detail::generator<int>{};
///     auto v = vast::detail::collect(g);
template <class T>
std::vector<T> collect(vast::detail::generator<T> g, size_t size_hint = 0) {
  return collect<std::vector<T>>(std::move(g), size_hint);
}

/// A utility function to collect the first result produced by a `generator<T>`
/// into a `std::optional<T>`.
template <class T>
std::optional<T> maybe(vast::detail::generator<T> g) {
  for (auto&& x : std::move(g))
    return x;
  return std::nullopt;
}

} // namespace vast::detail
