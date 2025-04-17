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

#include <boost/regex.hpp>

#include <optional>
#include <string_view>

namespace tenzir {

inline auto split_at_regex(std::string_view separator) {
  auto expr = boost::regex{separator.data(), separator.size(),
                           boost::regex_constants::no_except
                             | boost::regex_constants::optimize};
  return [exp = std::move(expr)](generator<chunk_ptr> input) mutable
           -> generator<std::optional<std::string_view>> {
    auto expr = std::move(exp); // NOLINT
    auto buffer = std::string{};
    auto consumed = true;
    boost::match_results<std::string::const_iterator> what;
    for (auto&& chunk : input) {
      if (not chunk or chunk->size() == 0) {
        co_yield std::nullopt;
        continue;
      }
      buffer.append(reinterpret_cast<const char*>(chunk->data()), chunk->size());
      const auto first = buffer.cbegin();
      auto current = first;
      auto begin = current + (consumed ? 0 : 1);
      while (begin <= buffer.end()
             and boost::regex_search(begin, buffer.cend(), what, expr)) {
        // Don't yield when we reached the end as a longer match could be found
        // with the subsequent characters at the beginning of the next chunk.
        if (buffer.cend() == what[0].second) {
          break;
        }
        co_yield std::string_view{current, what[0].first};
        // Move forward by at least one position in case the search did not
        // consume any characters.
        consumed = what[0].second > current;
        current = what[0].second;
        begin = what[0].second + (consumed ? 0 : 1);
      }
      buffer = buffer.substr(current - first);
      co_yield std::nullopt;
    }
    if (! buffer.empty()) {
      auto current = buffer.cbegin();
      auto begin = current + (consumed ? 0 : 1);
      while (begin <= buffer.end()
             and boost::regex_search(begin, buffer.cend(), what, expr)) {
        auto event = std::string_view{current, what[0].first};
        co_yield event;
        consumed = what[0].second > current;
        current = what[0].second;
        begin = what[0].second + (consumed ? 0 : 1);
      }
      if (buffer.cend() != current) {
        co_yield std::string_view{current, buffer.cend()};
      }
    }
  };
}

} // namespace tenzir
