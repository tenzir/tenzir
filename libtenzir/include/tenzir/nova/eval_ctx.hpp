//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"

namespace tenzir::nova {

/// The services one evaluation borrows; prepared evaluators retain none.
/// Handed to `Evaluator::eval` at the boundary, where no `EvalRun` — and
/// hence no `EvalFrame` — exists yet. Everything below that boundary uses
/// `EvalFrame`, which carries this context along with the input and the rows
/// to produce.
class EvalCtx {
public:
  explicit EvalCtx(diagnostic_handler& dh) : dh_{dh} {
  }

  auto dh() const -> diagnostic_handler& {
    return dh_;
  }

  explicit(false) operator diagnostic_handler&() const {
    return dh_;
  }

private:
  diagnostic_handler& dh_;
};

} // namespace tenzir::nova
