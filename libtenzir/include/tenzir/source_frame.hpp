//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"

namespace tenzir {

/// Resolve a substituted argument's position to the call frame where its
/// expression was written. Substitution stamps it with the callee's call ID
/// but preserves its original file and offsets.
inline auto in_own_frame(location source, SourceMap const& sources)
  -> location {
  auto id = source.callsite_index;
  while (auto call = sources.call_site(id)) {
    if (call->source_index == source.source_index) {
      source.callsite_index = call->callsite_index;
      break;
    }
    // Parents were registered before their children.
    if (call->callsite_index >= id) {
      break;
    }
    id = call->callsite_index;
  }
  return source;
}

} // namespace tenzir
