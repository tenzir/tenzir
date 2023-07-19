//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/mac.hpp"

namespace tenzir {

mac::mac() {
  bytes_.fill(std::byte{0});
}

auto mac::oui() const -> std::span<const std::byte, 3> {
  return as_bytes(*this).subspan<0, 3>();
}

auto mac::nic() const -> std::span<const std::byte, 3> {
  return as_bytes(*this).subspan<3, 3>();
}

auto mac::universal() const -> bool {
  constexpr auto mask = std::byte{0b00000010};
  return (bytes_[2] & mask) == mask;
}

auto mac::unicast() const -> bool {
  constexpr auto mask = std::byte{0b00000001};
  return (bytes_[2] & mask) == mask;
}

} // namespace tenzir
