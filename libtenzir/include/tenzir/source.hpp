//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/string.hpp"
#include "tenzir/location.hpp"

#include <atomic>
#include <string>

namespace tenzir {

struct SourceMap::Source {
private:
  struct Token {};

public:
  /// Creates a new imutable Source object with a unique ID in it.
  static auto new_source(std::string text, std::string origin, bool eager_split)
    -> Arc<const Source> {
    static auto id_generator = std::atomic<SourceId>{1};
    const auto id = id_generator.fetch_add(1);
    auto r = std::make_shared<Source>(id, std::move(text), std::move(origin),
                                      eager_split, Token{});
    return Arc<const Source>::from_non_null(std::move(r));
  }

  /// The source id used in `location::source_index`.
  SourceId index = 0;

  /// The TQL source text.
  std::string text;

  /// A description of where the text comes from, e.g., `<input>`, a config
  /// file path, or a package name.
  std::string origin;

  /// `text` already split into lines(views). The views point into `text`.
  Option<std::vector<std::string_view>> lines;

  Source(SourceId index, std::string text, std::string origin, bool eager_split,
         Token)
    : index{index},
      text{std::move(text)},
      origin{std::move(origin)},
      lines{maybe_split(text, eager_split)} {
  }

private:
  static auto maybe_split(const std::string& text, bool eager_split)
    -> Option<std::vector<std::string_view>> {
    if (not eager_split) {
      return None{};
    }
    auto split = detail::split(text, "\n");
    return std::move(split);
  }
};

} // namespace tenzir
