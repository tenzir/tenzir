//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/ether_type.hpp"

#include "vast/detail/byteswap.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace vast {

ether_type as_ether_type(std::span<const std::byte, 2> octets) {
  auto ptr = reinterpret_cast<const uint16_t*>(std::launder(octets.data()));
  return static_cast<ether_type>(detail::to_host_order(*ptr));
}

} // namespace vast
