//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

namespace tenzir {

class blob : public std::vector<std::byte> {
public:
  using super = std::vector<std::byte>;
  using super::super;

  explicit blob(std::span<const std::byte> span)
    : super(span.begin(), span.end()) {
  }

  friend constexpr auto operator+(blob l, const blob& r) -> blob {
    return l += r;
  }

  constexpr auto operator+=(const blob& r) -> blob& {
    insert(end(), r.begin(), r.end());
    return *this;
  }
};

} // namespace tenzir
