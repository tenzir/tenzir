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

#ifndef VAST_SYSTEM_CONFIGURATION_HPP
#define VAST_SYSTEM_CONFIGURATION_HPP

#include <string>
#include <vector>

#include <caf/actor_system_config.hpp>

namespace vast {
namespace system {

/// Bundles all configuration parameters of a VAST system.
class configuration : public caf::actor_system_config {
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
};

} // namespace system
} // namespace vast

#endif
