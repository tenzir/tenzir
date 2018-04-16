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

#include <memory>
#include <string>
#include <string_view>

#include "vast/command.hpp"

#include "vast/system/configuration.hpp"

namespace vast::system {

class application {
public:
  /// Default root command for VAST applications.
  class root_command : public command {
  public:
      root_command();

  protected:
    proceed_result proceed(caf::actor_system& sys, option_map& options,
                           const_iterator, const_iterator) override;

  private:
    /// Directory for persistent state.
    std::string dir;

    /// VAST server.
    std::string endpoint;

    /// The unique ID of this node.
    std::string id;

    /// Spawns a local node instead of connecting to a server if set.
    bool spawn_local;

    /// Print version and exit if set.
    bool print_version;
  };

  /// Constructs an application.
  /// @param cfg The VAST system configuration.
  application();

  /// Adds a command to the application or overrides an existing mapping.
  template <class T, class... Ts>
  T* add(std::string_view name, Ts&&... xs) {
    return root_.add<T>(name, std::forward<Ts>(xs)...);
  }

  /// Starts the application and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys, command::const_iterator args_begin,
          command::const_iterator args_end);

private:
  root_command root_;
};

} // namespace vast::system

