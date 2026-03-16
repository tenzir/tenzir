//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/command.hpp>

#include <caf/actor_system.hpp>
#include <web/fwd.hpp>

namespace tenzir::plugins::web {

auto generate_token_command(const tenzir::invocation&, caf::actor_system&)
  -> caf::message;

} // namespace tenzir::plugins::web
