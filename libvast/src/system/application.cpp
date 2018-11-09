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

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/banner.hpp"
#include "vast/logger.hpp"

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/data.hpp"

#include "vast/system/application.hpp"

#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"

using std::string;

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast::system {

application::application() {
  // TODO: this function has side effects...should we put it elsewhere where
  // it's explicit to the user? Or perhaps make whatever this function does
  // simply a configuration option and use it later?
  detail::adjust_resource_consumption();
}

} // namespace vast::system
