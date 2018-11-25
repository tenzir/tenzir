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

#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>

#include <caf/config_option_set.hpp>
#include <caf/error.hpp>
#include <caf/fwd.hpp>

namespace vast {

/// A named command with optional children.
class command {
public:
  // -- member types -----------------------------------------------------------

  /// Iterates over CLI arguments.
  using argument_iterator = std::vector<std::string>::const_iterator;

  /// Manages command objects.
  using owning_ptr = std::unique_ptr<command>;

  /// Stores child commands.
  using children_list = std::vector<std::unique_ptr<command>>;

  /// Delegates to the command implementation logic.
  using fun = caf::message (*)(const command&, caf::actor_system&,
                               caf::config_value_map&, argument_iterator,
                               argument_iterator);

  // -- member variables -------------------------------------------------------

  command* parent = nullptr;

  fun run = nullptr;

  std::string_view name;

  std::string_view description;

  caf::config_option_set options = opts();

  children_list children;

  // -- factory functions ------------------------------------------------------

  /// Creates a config option set pre-initialized with a help option.
  static caf::config_option_set opts();

  /// Adds a new subcommand.
  /// @returns a pointer to the new subcommand.
  command* add(fun child_run, std::string_view child_name,
               std::string_view child_description,
               caf::config_option_set child_options = {});
};

/// Runs the command and blocks until execution completes.
/// @returns a type-erased result or a wrapped `caf::error`.
/// @relates command
caf::message run(const command& cmd, caf::actor_system& sys,
                 command::argument_iterator first,
                 command::argument_iterator last);

/// Runs the command and blocks until execution completes.
/// @returns a type-erased result or a wrapped `caf::error`.
/// @relates command
caf::message run(const command& cmd, caf::actor_system& sys,
                 const std::vector<std::string>& args);

/// Runs the command and blocks until execution completes.
/// @returns a type-erased result or a wrapped `caf::error`.
/// @relates command
caf::message run(const command& cmd, caf::actor_system& sys,
                 caf::config_value_map& options,
                 command::argument_iterator first,
                 command::argument_iterator last);

/// Runs the command and blocks until execution completes.
/// @returns a type-erased result or a wrapped `caf::error`.
/// @relates command
caf::message run(const command& cmd, caf::actor_system& sys,
                 caf::config_value_map& options,
                 const std::vector<std::string>& args);

/// Returns the full name of `cmd`, i.e., its own name prepended by all parent
/// names.
std::string full_name(const command& cmd);

/// Prints the helptext for `cmd` to `out`.
void helptext(const command& cmd, std::ostream& out);

/// Returns the helptext for `cmd`.
std::string helptext(const command& cmd);

/// Applies `fun` to `cmd` and each of its children, recursively.
template <class F>
void for_each(const command& cmd, F fun) {
  fun(cmd);
  for (auto& ptr : cmd.children)
    for_each(*ptr, fun);
}

} // namespace vast

