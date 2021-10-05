//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concepts.hpp"

#include <array>
#include <cstddef>
#include <span>

namespace vast {

/// A fixed-size hash digest in big-endian.
/// @tparam The number of bytes.
template <size_t Bytes>
struct digest : std::array<std::byte, Bytes> {
  explicit digest(std::array<std::byte, Bytes> xs)
    : std::array<std::byte, Bytes>{xs} {
  }

  explicit digest(std::span<std::byte, Bytes> xs) {
    std::memcpy(this->data(), xs.data(), Bytes);
  }

  template <concepts::integral T>
    requires(Bytes % sizeof(T) == 0)
  explicit digest(std::span<T, Bytes / sizeof(T)> xs) {
    std::memcpy(this->data(), xs.data(), Bytes);
  }

  template <concepts::integral T>
    requires(sizeof(T) == Bytes)
  explicit digest(T x) {
    std::memcpy(this->data(), &x, Bytes);
  }

  template <class T>
    requires(sizeof(T) == Bytes)
  friend T as(const digest& x) {
    return *reinterpret_cast<const T*>(x.data());
  }

  friend auto as_bytes(const digest& x) {
    return std::span<const std::byte, Bytes>{x};
  }
};

} // namespace vast
