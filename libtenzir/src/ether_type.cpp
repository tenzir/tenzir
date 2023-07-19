//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ether_type.hpp"

#include "tenzir/detail/byteswap.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace tenzir {

auto as_ether_type(std::span<const std::byte, 2> octets) -> ether_type {
  auto ptr = reinterpret_cast<const uint16_t*>(std::launder(octets.data()));
  return static_cast<ether_type>(detail::to_host_order(*ptr));
}

} // namespace tenzir
