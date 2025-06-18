//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/chunk.hpp"
#include "tenzir/generator.hpp"

#include <optional>
#include <string_view>

namespace tenzir {

inline auto
split_at_string(std::string_view separator, bool include_separator = false) {
  return [separator, include_separator](generator<chunk_ptr> input) mutable
           -> generator<std::optional<std::string_view>> {
    auto buffer = std::string{};
    for (auto&& chunk : input) {
      if (not chunk or chunk->size() == 0) {
        co_yield std::nullopt;
        continue;
      }
      buffer.append(reinterpret_cast<const char*>(chunk->data()),
                    chunk->size());
      std::size_t current = 0;
      std::size_t pos = 0;
      while ((pos = buffer.find(separator, current)) != std::string::npos) {
        // Don't yield when we reached the end as a longer match could be found
        // with the subsequent characters at the beginning of the next chunk.
        if (pos + separator.size() == buffer.size()) {
          break;
        }
        if (include_separator) {
          co_yield std::string_view{buffer.data() + current,
                                    pos + separator.size() - current};
        } else {
          co_yield std::string_view{buffer.data() + current, pos - current};
        }
        current = pos + separator.size();
      }
      buffer = buffer.substr(current);
      co_yield std::nullopt;
    }
    if (not buffer.empty()) {
      std::size_t current = 0;
      std::size_t pos = 0;
      while ((pos = buffer.find(separator, current)) != std::string::npos) {
        if (include_separator) {
          auto event = std::string_view{buffer.data() + current,
                                        pos + separator.size() - current};
          co_yield event;
        } else {
          auto event = std::string_view{buffer.data() + current, pos - current};
          co_yield event;
        }
        current = pos + separator.size();
      }
      if (current < buffer.size()) {
        co_yield std::string_view{buffer.data() + current,
                                  buffer.size() - current};
      }
    }
  };
}

} // namespace tenzir
