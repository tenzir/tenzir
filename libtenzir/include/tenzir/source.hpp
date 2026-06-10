//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"

#include <atomic>
#include <string>

namespace tenzir {

struct SourceMap::Source {
  /// Returns the next globally unique source id.
  ///
  /// The main source keeps using id `0`, so global source ids start at `1`.
  static auto next_index() -> SourceId {
    static auto next = std::atomic<SourceId>{1};
    return next.fetch_add(1);
  }

  /// The source id used in `location::source_index`.
  SourceId index = 0;

  /// The TQL source text.
  std::string text;

  /// A description of where the text comes from, e.g., `<input>`, a config
  /// file path, or a package name.
  std::string origin;

  friend auto inspect(auto& f, Source& x) -> bool {
    return f.object(x).pretty_name("source").fields(
      f.field("index", x.index), f.field("text", x.text),
      f.field("origin", x.origin));
  }
};

} // namespace tenzir
