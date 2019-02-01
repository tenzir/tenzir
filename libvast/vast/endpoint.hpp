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

namespace vast {

/// A transport-layer endpoint consisting of host and port.
struct endpoint {
  std::string host;   ///< The hostname or IP address.
  uint64_t port = 0;  ///< The transport-layer port.
};

/// @returns an endpoint with values from the default settings.
/// @relates endpoint make_endpoint
endpoint make_default_endpoint();

} // namespace vast
