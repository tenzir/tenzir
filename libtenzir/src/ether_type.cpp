//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ether_type.hpp"

#include <cstddef>
#include <span>

namespace tenzir {

auto as_ether_type(std::span<const std::byte, 2> octets) -> ether_type {
  return ether_type((uint16_t(octets[0]) << 8) | uint16_t(octets[1]));
}

} // namespace tenzir
