//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <ranges>

namespace tenzir::detail {

template <class Int, class T>
class enumerator {
public:
  template <class It>
  class iterator {
  public:
    explicit iterator(It it) : it_{std::move(it)} {
    }

    auto operator*() const -> decltype(auto) {
      return std::pair<Int, decltype((*it_))>{count_, *it_};
    }

    void operator++() {
      ++it_;
      ++count_;
    }

    template <class It2>
    auto operator!=(const It2& other) const -> bool {
      return it_ != other;
    }

  private:
    It it_;
    Int count_ = 0;
  };

  template <class It>
  iterator(It) -> iterator<It>;

  explicit enumerator(T&& x) : x_{std::forward<T>(x)} {
  }

  auto begin() {
    return iterator{std::ranges::begin(x_)};
  }

  auto begin() const {
    return iterator{std::ranges::begin(x_)};
  }

  auto end() {
    return std::ranges::end(x_);
  }

  auto end() const {
    return std::ranges::end(x_);
  }

private:
  T x_;
};

// TODO: Make sure these overloads are what we want.
template <class Int = size_t, class T>
auto enumerate(T&& x) -> enumerator<Int, T> {
  return enumerator<Int, T>(std::forward<T>(x));
}

template <class Int = size_t, class T>
auto enumerate(T& x) -> enumerator<Int, T&> {
  return enumerator<Int, T&>(x);
}

} // namespace tenzir::detail
