//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("set source spans combine assignments only within one invocation") {
  auto first = ast::assignment{};
  first.location = location{10, 15, 7, 2};
  auto second = ast::assignment{};
  second.location = location{20, 25, 7, 2};
  check_eq((ir::SetIr{{first, second}}.main_location()),
           (location{10, 25, 7, 2}));
  second.location.callsite_index = 3;
  check_eq((ir::SetIr{{first, second}}.main_location()), first.location);
  second.location = location{20, 25, 8, 2};
  check_eq((ir::SetIr{{first, second}}.main_location()), first.location);
  check_eq(ir::SetIr{}.main_location(), location::unknown);
}
