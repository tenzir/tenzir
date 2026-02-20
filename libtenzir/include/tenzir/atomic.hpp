//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <atomic>

namespace tenzir {

template <class T>
class Atomic {
public:
  using value_type = T;
  static constexpr auto is_always_lock_free
    = std::atomic<T>::is_always_lock_free;

  constexpr Atomic() noexcept = default;

  constexpr explicit Atomic(T desired) noexcept : data_{desired} {
  }

  constexpr Atomic(const Atomic& other) noexcept
    : data_{other.load(std::memory_order_relaxed)} {
  }

  constexpr Atomic(Atomic&& other) noexcept
    : data_{other.load(std::memory_order_relaxed)} {
  }

  auto operator=(const Atomic& other) noexcept -> Atomic& {
    store(other.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }

  auto operator=(Atomic&& other) noexcept -> Atomic& {
    store(other.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }

  ~Atomic() = default;

  auto is_lock_free() const noexcept -> bool {
    return data_.is_lock_free();
  }

  void store(T desired, std::memory_order order
                        = std::memory_order_seq_cst) noexcept {
    data_.store(desired, order);
  }

  auto load(std::memory_order order = std::memory_order_seq_cst) const noexcept
    -> T {
    return data_.load(order);
  }

  auto exchange(T desired, std::memory_order order
                           = std::memory_order_seq_cst) noexcept -> T {
    return data_.exchange(desired, order);
  }

  auto compare_exchange_weak(T& expected, T desired, std::memory_order success,
                             std::memory_order failure) noexcept -> bool {
    return data_.compare_exchange_weak(expected, desired, success, failure);
  }

  auto compare_exchange_weak(T& expected, T desired,
                             std::memory_order order
                             = std::memory_order_seq_cst) noexcept -> bool {
    return data_.compare_exchange_weak(expected, desired, order);
  }

  auto
  compare_exchange_strong(T& expected, T desired, std::memory_order success,
                          std::memory_order failure) noexcept -> bool {
    return data_.compare_exchange_strong(expected, desired, success, failure);
  }

  auto compare_exchange_strong(T& expected, T desired,
                               std::memory_order order
                               = std::memory_order_seq_cst) noexcept -> bool {
    return data_.compare_exchange_strong(expected, desired, order);
  }

  void wait(T old, std::memory_order order
                   = std::memory_order_seq_cst) const noexcept {
    data_.wait(old, order);
  }

  void notify_one() noexcept {
    data_.notify_one();
  }

  void notify_all() noexcept {
    data_.notify_all();
  }

  template <class Arg>
    requires requires(std::atomic<T>& a, Arg arg, std::memory_order o) {
      a.fetch_add(arg, o);
    }
  auto fetch_add(Arg arg, std::memory_order order
                          = std::memory_order_seq_cst) noexcept {
    return data_.fetch_add(arg, order);
  }

  template <class Arg>
    requires requires(std::atomic<T>& a, Arg arg, std::memory_order o) {
      a.fetch_sub(arg, o);
    }
  auto fetch_sub(Arg arg, std::memory_order order
                          = std::memory_order_seq_cst) noexcept {
    return data_.fetch_sub(arg, order);
  }

  template <class Arg>
    requires requires(std::atomic<T>& a, Arg arg, std::memory_order o) {
      a.fetch_and(arg, o);
    }
  auto fetch_and(Arg arg, std::memory_order order
                          = std::memory_order_seq_cst) noexcept {
    return data_.fetch_and(arg, order);
  }

  template <class Arg>
    requires requires(std::atomic<T>& a, Arg arg, std::memory_order o) {
      a.fetch_or(arg, o);
    }
  auto fetch_or(Arg arg, std::memory_order order
                         = std::memory_order_seq_cst) noexcept {
    return data_.fetch_or(arg, order);
  }

  template <class Arg>
    requires requires(std::atomic<T>& a, Arg arg, std::memory_order o) {
      a.fetch_xor(arg, o);
    }
  auto fetch_xor(Arg arg, std::memory_order order
                          = std::memory_order_seq_cst) noexcept {
    return data_.fetch_xor(arg, order);
  }

private:
  std::atomic<T> data_;
};

} // namespace tenzir
