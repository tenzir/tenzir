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

#include "vast/test/test.hpp"

#include "vast/error.hpp"
#include "vast/path.hpp"

namespace fixtures {

struct filesystem {
  filesystem() {
    // Fresh afresh.
    rm(directory);
    if (auto err = mkdir(directory))
      // error is non-recoverable, so we just abort
      FAIL(vast::render(err));
  }

  vast::path directory = "vast-unit-test";
};

} // namespace fixtures
