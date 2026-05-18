//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/folly_init.hpp"

#include <folly/init/Init.h>

namespace tenzir {

struct folly_init_guard::impl {
  int argc = 1;
  char** argv;
  folly::Init init;

  explicit impl(char** outer_argv)
    : argv{outer_argv},
      init{&argc, &argv,
           folly::InitOptions{}.useGFlags(false).removeFlags(false)} {
  }
};

folly_init_guard::folly_init_guard(char** argv)
  : impl_{std::make_unique<impl>(argv)} {
}

folly_init_guard::~folly_init_guard() = default;

} // namespace tenzir
