//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugin/base.hpp"
#include "tenzir/command.hpp"

#include <memory>
#include <utility>

namespace tenzir {

// -- command plugin -----------------------------------------------------------

/// A base class for plugins that add commands.
/// @relates plugin
class command_plugin : public virtual plugin {
public:
  /// Creates additional commands.
  /// @note Tenzir calls this function before initializing the plugin, which
  /// means that this function cannot depend on any plugin state. The logger
  /// is unavailable when this function is called.
  [[nodiscard]] virtual auto make_command() const
    -> std::pair<std::unique_ptr<command>, command::factory>
    = 0;
};

} // namespace tenzir
