//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <array>
#include <cstddef>
#include <span>

namespace tenzir {

template <size_t Size>
auto as_bytes(const void* data) noexcept {
  return std::span<const std::byte, Size>{
    reinterpret_cast<const std::byte*>(data), Size};
}

template <size_t Size>
auto as_writeable_bytes(void* data) noexcept {
  return std::span<std::byte, Size>{reinterpret_cast<std::byte*>(data), Size};
}

inline auto
as_bytes(const void* data, size_t size) noexcept -> std::span<const std::byte> {
  return {reinterpret_cast<const std::byte*>(data), size};
}

inline auto
as_writeable_bytes(void* data, size_t size) noexcept -> std::span<std::byte> {
  return {reinterpret_cast<std::byte*>(data), size};
}

template <std::integral T, size_t N>
constexpr auto as_bytes(const std::array<T, N>& xs) noexcept
  -> std::span<const std::byte, N * sizeof(T)> {
  const auto* const data = reinterpret_cast<const std::byte*>(xs.data());
  return std::span<const std::byte, N * sizeof(T)>{data, N * sizeof(T)};
}

template <std::integral T, size_t N>
constexpr auto as_writeable_bytes(std::array<T, N>& xs) noexcept
  -> std::span<std::byte, N * sizeof(T)> {
  auto* const data = reinterpret_cast<std::byte*>(xs.data());
  return std::span<std::byte, N * sizeof(T)>{data, N * sizeof(T)};
}

template <concepts::byte_container Buffer>
constexpr auto
as_bytes(const Buffer& xs) noexcept -> std::span<const std::byte> {
  const auto* const data = reinterpret_cast<const std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

template <concepts::byte_container Buffer>
constexpr auto as_writeable_bytes(Buffer& xs) noexcept -> std::span<std::byte> {
  auto* const data = reinterpret_cast<std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

} // namespace tenzir
