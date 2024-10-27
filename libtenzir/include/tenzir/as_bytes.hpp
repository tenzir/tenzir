//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/detail/assert.hpp"

#include <array>
#include <cstddef>
#include <span>

namespace tenzir {

template <concepts::number Number>
constexpr auto as_bytes(const Number& x) noexcept
  -> std::span<const std::byte, sizeof(Number)> {
  const auto* data = reinterpret_cast<const std::byte*>(&x);
  return std::span<const std::byte, sizeof(Number)>{data, sizeof(Number)};
}

template <concepts::number Number>
constexpr auto as_writeable_bytes(Number& x) noexcept
  -> std::span<std::byte, sizeof(Number)> {
  auto* data = reinterpret_cast<std::byte*>(&x);
  return std::span<std::byte, sizeof(Number)>{data, sizeof(Number)};
}

template <size_t Extent = std::dynamic_extent>
auto as_bytes(const void* data, size_t size) noexcept
  -> std::span<const std::byte, Extent> {
  if constexpr (Extent != std::dynamic_extent) {
    TENZIR_ASSERT(size >= Extent);
  }
  return std::span<const std::byte, Extent>{
    reinterpret_cast<const std::byte*>(data), size};
}

template <size_t Extent = std::dynamic_extent>
auto as_writeable_bytes(void* data, size_t size) noexcept
  -> std::span<std::byte, Extent> {
  if constexpr (Extent != std::dynamic_extent) {
    TENZIR_ASSERT(size >= Extent);
  }
  return std::span<std::byte, Extent>{reinterpret_cast<std::byte*>(data), size};
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
