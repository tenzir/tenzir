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

#ifndef VAST_SYSTEM_APPLICATION_HPP
#define VAST_SYSTEM_APPLICATION_HPP

#include <memory>
#include <string>

#include "vast/system/configuration.hpp"

namespace vast::system {

class application {
public:
  /// Constructs an application.
  /// @param cfg The VAST system configuration.
  application(const configuration& cfg);

  /// Starts the application and blocks until execution completes.
  /// @returns An exit code suitable for returning from main.
  int run(caf::actor_system& sys);

private:
  const configuration& config_;
  //command program_;
};

} // namespace vast::system

#endif
