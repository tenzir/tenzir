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

#include "vast/detail/backtrace.hpp"

#include "vast/config.hpp"

#if VAST_ENABLE_BACKTRACE
#  include <execinfo.h>
#  include <unistd.h>
#endif

namespace vast::detail {

void backtrace() {
#if VAST_ENABLE_BACKTRACE
  ::fprintf(stderr, "backtrace:\n");
  void* vast_array[10];
  auto vast_bt_size = ::backtrace(vast_array, 10);
  ::backtrace_symbols_fd(vast_array, vast_bt_size, STDERR_FILENO);
#endif
}

} // namespace vast::detail
