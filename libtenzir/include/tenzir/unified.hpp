//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// The `tenzir` dispatcher links no libtenzir, but includes this header as well,
// so it depends on the standard library only.

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string_view>

namespace tenzir {

/// The first major version whose `tenzir` uses the unified command interface
/// when `TENZIR_UNIFIED` is unset or empty.
inline constexpr auto unified_major_version = std::uint64_t{7};

/// Whether `tenzir` of major version `major` runs with the unified command
/// interface. An unset or empty `TENZIR_UNIFIED` selects the unified interface
/// from `unified_major_version` on, `0`, `false` or `no` in any case the
/// previous interface, and any other value the unified one. The `tenzir`
/// dispatcher decides with this function and passes the variable on, so that
/// the engine decides the same.
inline auto unified_mode(std::uint64_t major) -> bool {
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  auto const* value = std::getenv("TENZIR_UNIFIED");
  if (value == nullptr or *value == '\0') {
    return major >= unified_major_version;
  }
  auto const equals_ignoring_case = [](std::string_view text,
                                       std::string_view expected) {
    if (text.size() != expected.size()) {
      return false;
    }
    for (auto i = std::size_t{0}; i < text.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(text[i])) != expected[i]) {
        return false;
      }
    }
    return true;
  };
  auto const text = std::string_view{value};
  return not equals_ignoring_case(text, "0")
         and not equals_ignoring_case(text, "false")
         and not equals_ignoring_case(text, "no");
}

} // namespace tenzir
