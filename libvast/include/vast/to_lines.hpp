//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/chunk.hpp"
#include "vast/generator.hpp"

namespace vast {

/// Transforms a sequence of bytes into a sequence of lines. The returned
/// sequence may spuriously contain `std::nullopt`, which shall be ignored. An
/// empty line is translated into an empty string view.
inline auto to_lines(generator<chunk_ptr> input)
  -> generator<std::optional<std::string_view>> {
  auto buffer = std::string{};
  bool ended_on_linefeed = false;
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    if (ended_on_linefeed && *begin == '\n') {
      ++begin;
    };
    ended_on_linefeed = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' && *current != '\r') {
        continue;
      }
      if (buffer.empty()) {
        co_yield std::string_view{begin, current};
      } else {
        buffer.append(begin, current);
        co_yield buffer;
        buffer.clear();
      }
      if (*current == '\r') {
        auto next = current + 1;
        if (next == end) {
          ended_on_linefeed = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer.append(begin, end);
    co_yield std::nullopt;
  }
  if (!buffer.empty()) {
    co_yield std::move(buffer);
  }
}

} // namespace vast
