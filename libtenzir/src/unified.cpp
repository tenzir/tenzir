//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/unified.hpp"

#include "tenzir/detail/env.hpp"

#include <algorithm>
#include <cctype>

namespace tenzir {

// The dispatcher in `tenzir/dispatcher.cpp` links no libtenzir, so it has its
// own copy of this rule. The two have to agree.
auto unified_mode() -> bool {
  auto value = detail::getenv("TENZIR_UNIFIED");
  if (not value or value->empty()) {
    return false;
  }
  std::ranges::transform(*value, value->begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return *value != "0" and *value != "false" and *value != "no";
}

} // namespace tenzir
