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

#ifndef VAST_COMMAND_HPP
#define VAST_COMMAND_HPP

#include <caf/fwd.hpp>

#include <memory>
#include <string>

#include "vast/error.hpp"
#include "vast/data.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#include "vast/detail/steady_map.hpp"
#include "vast/detail/string.hpp"

namespace vast {

/// A command with options that can contain further sub-commands.
struct command {
  /// An option of a command.
  struct option {
    template <class T = bool>
    static std::pair<std::string, option>
    make(const std::string& tag, std::string desc, T x = {}) {
      auto s = detail::split_to_str(tag, ",");
      std::string shortcut;
      if (s.size() >= 2)
        shortcut = s[1][0];
      return {s[0], {std::move(shortcut), std::move(desc),
                    data{std::forward<T>(x)}}};
    }

    std::string shortcut;
    std::string description;
    data value;
  };

  using ptr = std::shared_ptr<command>;

  /// The callback type when a command gets dispatched. The first argument is a
  /// reference to the current command and the second argument the list of
  /// remaining arguments.
  using callback_type = std::function<
    void(const command&, std::vector<std::string>)
  >;

  /// Constructs a command.
  /// @param desc The description of the command.
  static ptr make(std::string desc = "");

  /// Defines an option for the command.
  /// @tparam T The type of the option value.
  /// @param tag The tag for the long and optionally short switch.
  /// @param desc The description of the option.
  /// @param x The default value for the option.
  template <class T = bool>
  command& opt(const std::string& tag, std::string desc, T x = {}) {
    options.insert(option::make(tag, std::move(desc), std::forward<T>(x)));
    return *this;
  }

  /// Defines a sub-command.
  /// @param name The name of the command.
  /// @param desc The description of the command.
  command& cmd(const std::string& name, std::string desc = "");

  /// Parses command line arguments and dispatches the contained command to the
  /// registered sub-command.
  /// @param args The command line arguments.
  void dispatch(const std::vector<std:string>& args) const;

  /// Defines a callback for this command.
  template <class F>
  void callback(F f) {
    callback_ = std::move(f);
  }

  /// Retrieves an option value.
  /// @param x The name of the option.
  /// @returns The value for *x* or `nullptr` if `x` is not a valid option.
  const data* get(const std::string& x) const;

  std::string description;
  detail::steady_map<std::string, option> options;
  detail::steady_map<std::string, ptr> commands;

private:
  callback_type callback_;
};

} // namespace vast

#endif
