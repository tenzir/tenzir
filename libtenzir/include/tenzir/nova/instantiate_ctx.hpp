//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"

namespace tenzir::nova {

/// Services used only while preparing expressions and function arguments.
class InstantiateCtx : base_ctx {
public:
  using base_ctx::base_ctx;
  using base_ctx::operator diagnostic_handler&;
  using base_ctx::operator const registry&;
};

} // namespace tenzir::nova
