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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <caf/actor_system_config.hpp>
#include <caf/config_option_set.hpp>
#include <caf/fwd.hpp>
#include <caf/message.hpp>
#include <caf/pec.hpp>

#include "vast/data.hpp"
#include "vast/error.hpp"

#include "vast/detail/steady_map.hpp"
#include "vast/detail/string.hpp"

namespace vast {

/// A top-level command.
class command {
public:
  // -- member types -----------------------------------------------------------

  /// Iterates over CLI arguments.
  using argument_iterator = std::vector<std::string>::const_iterator;

  /// Wraps the result of proceed.
  enum proceed_result {
    proceed_ok,
    stop_successful,
    stop_with_error
  };

  // -- constructors, destructors, and assignment operators --------------------

  command();

  command(command* parent, std::string_view name);

  virtual ~command();

  /// Runs the command and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, argument_iterator begin,
          argument_iterator end);

  /// Runs the command and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, caf::config_value_map& options,
          argument_iterator begin, argument_iterator end);


  /// Creates a summary of all option declarations and available commands.
  std::string usage() const;

  /// Returns the full name for this command.
  std::string full_name() const;

  /// Queries whether this command has no parent.
  inline bool is_root() const noexcept {
    return parent_ == nullptr;
  }

  /// Returns the root command.
  inline command& root() noexcept {
    return is_root() ? *this : parent_->root();
  }

  /// Returns the managed command name.
  inline std::string_view name() const noexcept {
    return name_;
  }

  /// Defines a sub-command.
  /// @param name The name of the command.
  /// @param xs The parameters required to construct the command.
  template <class T, class... Ts>
  T* add(std::string_view name, Ts&&... xs) {
    auto ptr = std::make_unique<T>(this, name, std::forward<Ts>(xs)...);
    auto result = ptr.get();
    if (!nested_.emplace(name, std::move(ptr)).second) {
      // FIXME: do not use exceptions.
      throw std::invalid_argument("name already exists");
    }
    return result;
  }

protected:
  /// Checks whether a command is ready to proceed, i.e., whether the
  /// configuration allows for calling `run_impl` or `run` on a nested command.
  virtual proceed_result proceed(caf::actor_system& sys,
                                 const caf::config_value_map& options,
                                 argument_iterator begin,
                                 argument_iterator end);

  virtual int run_impl(caf::actor_system& sys,
                       const caf::config_value_map& options,
                       argument_iterator begin, argument_iterator end);

  template <class T>
  void add_opt(std::string_view name, std::string_view description) {
    opts_.add<T>("global", name, description);
  }

private:
  std::string parse_error(caf::pec code, argument_iterator error_position,
                          argument_iterator begin, argument_iterator end) const;

  /// @pre `error_position != end`
  std::string unknown_subcommand_error(argument_iterator error_position,
                                       argument_iterator end) const;

  std::map<std::string_view, std::unique_ptr<command>> nested_;
  command* parent_;

  /// The user-provided name.
  std::string_view name_;

  /// List of all accepted options.
  caf::config_option_set opts_;
};

} // namespace vast

