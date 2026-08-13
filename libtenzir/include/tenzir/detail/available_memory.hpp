//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <cstdint>
#include <string>

namespace tenzir::detail {

struct available_memory_info {
  uint64_t bytes = 0;
  std::string source = {};
};

auto available_memory() -> Option<available_memory_info>;

} // namespace tenzir::detail
