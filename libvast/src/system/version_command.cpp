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

#include "vast/system/version_command.hpp"

#include <iostream>

#include <caf/message.hpp>

#include "vast/config.hpp"

namespace vast::system {

caf::message version_command(const command&, caf::actor_system&, caf::settings&,
                             command::argument_iterator,
                             command::argument_iterator) {
  std::cout << VAST_VERSION << std::endl;
  return caf::none;
}

} // namespace vast::system
