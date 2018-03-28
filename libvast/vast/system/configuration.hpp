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
#include <vector>

#include <caf/actor_system_config.hpp>

namespace vast::system {

class application;

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
  friend application;

public:
  /// Default-constructs a configuration.
  configuration();

  /// Constructs a configuration from the command line.
  /// @param argc The argument counter of `main`.
  /// @param argv The argument vector of `main`.
  configuration(int argc, char** argv);

  /// Constructs a configuration from a vector of string options.
  /// @param opts The vector with CAF options.
  configuration(const std::vector<std::string>& opts);

  // -- configuration options -------------------------------------------------

  /// The program command line, without --caf# arguments.
  std::vector<std::string> command_line;
};

} // namespace vast::system

