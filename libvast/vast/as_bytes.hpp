// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/type_traits.hpp"
#include "vast/span.hpp"

#include <cstddef>
#include <type_traits>

namespace vast {

template <class Buffer,
          class = std::enable_if_t<detail::is_byte_container_v<Buffer>>>
constexpr span<const std::byte> as_bytes(const Buffer& xs) noexcept {
  const auto data = reinterpret_cast<const std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

template <class Buffer,
          class = std::enable_if_t<detail::is_byte_container_v<Buffer>>>
constexpr span<std::byte> as_writeable_bytes(Buffer& xs) noexcept {
  const auto data = reinterpret_cast<std::byte*>(std::data(xs));
  return {data, std::size(xs)};
}

} // namespace vast
