//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/chunk.hpp"
#include "tenzir/generator.hpp"

namespace tenzir {

/// Converts a stream of chunks into a stream of strings by splitting the input
/// at null bytes. The returned sequence may spuriously contain `std::nullopt`,
/// which shall be ignored. Consecutive null bytes produce empty sting views.
inline auto split_nulls(generator<chunk_ptr> input)
  -> generator<std::optional<std::string_view>> {
  auto buffer = std::string{};
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\0') {
        continue;
      }
      if (buffer.empty()) {
        co_yield std::string_view{begin, current};
      } else {
        buffer.append(begin, current);
        co_yield buffer;
        buffer.clear();
      }
      begin = current + 1;
    }
    buffer.append(begin, end);
    co_yield std::nullopt;
  }
  if (!buffer.empty()) {
    co_yield buffer;
  }
}

} // namespace tenzir
