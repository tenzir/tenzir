//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/concepts.hpp"

#include <cstddef>
#include <span>
#include <type_traits>

namespace vast {

template <detail::byte_container Buffer>
constexpr std::span<const std::byte> as_bytes(const Buffer& xs) noexcept {
  const auto data = reinterpret_cast<const std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

template <detail::byte_container Buffer>
constexpr std::span<std::byte> as_writeable_bytes(Buffer& xs) noexcept {
  const auto data = reinterpret_cast<std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

} // namespace vast
