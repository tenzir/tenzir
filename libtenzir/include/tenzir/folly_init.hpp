//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <memory>

namespace tenzir {

/// RAII wrapper around `folly::Init` that hides folly headers and symbols
/// behind libtenzir. Consumers (plugin test binaries, the main `tenzir`
/// executable) can initialise folly without having to link folly directly,
/// which matters when folly is bundled as a static archive inside
/// libtenzir.so with no exported link interface.
class folly_init_guard {
public:
  /// Initialise folly singletons with the same options as the main
  /// `tenzir` binary: gflags disabled and `argv` left untouched.
  explicit folly_init_guard(char** argv);
  ~folly_init_guard();
  folly_init_guard(folly_init_guard const&) = delete;
  folly_init_guard& operator=(folly_init_guard const&) = delete;
  folly_init_guard(folly_init_guard&&) = delete;
  folly_init_guard& operator=(folly_init_guard&&) = delete;

private:
  // Held as a unique_ptr so the header doesn't need <folly/init/Init.h>.
  struct impl;
  std::unique_ptr<impl> impl_;
};

} // namespace tenzir
