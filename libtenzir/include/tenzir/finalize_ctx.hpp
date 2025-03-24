//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"

namespace tenzir {

/// Context when transforming the IR to `plan::operator_ptr`.
class finalize_ctx {
public:
  // TODO: Right now, this is just the base context. If we know that we don't
  // need anything else then we could delete this.
  explicit finalize_ctx(base_ctx ctx) : ctx_{ctx} {
  }

  auto dh() -> diagnostic_handler& {
    return ctx_;
  }

  explicit(false) operator diagnostic_handler&() {
    return ctx_;
  }

private:
  base_ctx ctx_;
};

} // namespace tenzir
