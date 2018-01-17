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

#include "vast/config.hpp"
#include "vast/detail/adjust_resource_consumption.hpp"

#ifdef VAST_MACOS
#include <sys/resource.h> // setrlimit
#endif

namespace vast {
namespace detail {

bool adjust_resource_consumption() {
#ifdef VAST_MACOS
  auto rl = ::rlimit{4096, 8192};
  auto result = ::setrlimit(RLIMIT_NOFILE, &rl);
  return result == 0;
#endif
  return true;
}

} // namespace detail
} // namespace vast
