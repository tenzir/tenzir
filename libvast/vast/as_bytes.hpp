//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concepts.hpp"

#include <array>
#include <cstddef>
#include <span>
#include <type_traits>

namespace vast {

inline std::span<const std::byte>
as_bytes(const void* data, size_t size) noexcept {
  return {reinterpret_cast<const std::byte*>(data), size};
}

inline std::span<std::byte>
as_writeable_bytes(void* data, size_t size) noexcept {
  return {reinterpret_cast<std::byte*>(data), size};
}

template <concepts::integral T, size_t N>
constexpr std::span<const std::byte, N * sizeof(T)>
as_bytes(const std::array<T, N>& xs) noexcept {
  const auto data = reinterpret_cast<const std::byte*>(xs.data());
  return std::span<const std::byte, N * sizeof(T)>{data, N * sizeof(T)};
}

template <concepts::integral T, size_t N>
constexpr std::span<std::byte, N * sizeof(T)>
as_writeable_bytes(std::array<T, N>& xs) noexcept {
  const auto data = reinterpret_cast<std::byte*>(xs.data());
  return std::span<std::byte, N * sizeof(T)>{data, N * sizeof(T)};
}

template <concepts::byte_container Buffer>
constexpr std::span<const std::byte> as_bytes(const Buffer& xs) noexcept {
  const auto data = reinterpret_cast<const std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

template <concepts::byte_container Buffer>
constexpr std::span<std::byte> as_writeable_bytes(Buffer& xs) noexcept {
  const auto data = reinterpret_cast<std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

} // namespace vast
