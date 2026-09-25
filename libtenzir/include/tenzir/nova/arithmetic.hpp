//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/eval.hpp"

#include <array>

namespace tenzir::nova {

/// Subtracts the right array from the left array for the rows selected by the
/// frame.
auto evaluate_subtraction(EvalFrame frame, std::array<Array<Data>, 2> args,
                          location loc) -> Array<Data>;

} // namespace tenzir::nova
