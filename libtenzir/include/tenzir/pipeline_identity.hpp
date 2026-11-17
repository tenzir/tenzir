//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>

namespace tenzir {

/// Who the host says a pipeline is.
struct PipelineIdentity {
  /// The ID the host gave the pipeline.
  std::string id;
  /// Whether the pipeline runs only once, so that no later run continues where
  /// this one stopped.
  bool adhoc = false;
};

} // namespace tenzir
