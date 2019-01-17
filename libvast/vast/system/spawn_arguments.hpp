/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <string>

#include "caf/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/fwd.hpp"

namespace vast::system {

/// Wraps arguments for spawn functions.
struct spawn_arguments {
  /// Current command executed by the node actor.
  const command& cmd;

  /// Path to persistent node state.
  const path& dir;

  /// Label for the new component.
  const std::string& label;

  /// User-defined options for spawning the component.
  const caf::settings& options;

  /// Iterator to the first CLI argument.
  cli_argument_iterator first;

  /// Past-the-end iterator for CLI arguments.
  cli_argument_iterator last;

  /// Returns whether CLI arguments are empty.
  bool empty() const noexcept {
    return first == last;
  }
};

/// Attempts to parse `[args.first, args.last)` as ::expression and returns a
/// normalized and validated version of that expression on success.
caf::expected<expression> normalized_and_valided(const spawn_arguments& args);

/// Attemps to read a schema file and parse its content. Can either 1) return
/// nothing if the user didn't specifiy a schema file in `args.options`, 2)
/// produce a valid schema, or 3) run into an error.
caf::expected<caf::optional<schema>> read_schema(const spawn_arguments& args);

/// Generates an error for unexpected CLI arguments in `args`.
caf::error unexpected_arguments(const spawn_arguments& args);

} // namespace vast::system
