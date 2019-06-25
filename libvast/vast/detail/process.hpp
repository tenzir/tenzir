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

#include "vast/filesystem.hpp"
#include <caf/optional.hpp>

namespace vast::detail {

/// Get the path of a shared library.
/// @returns The filesystem path to the library mapped at address addr, or none
///          if the running process was not created from a dynamic executable.
caf::optional<path> objectpath(const void* addr);

inline caf::optional<path> objectpath() {
  struct dummy {
    static void fn() {
    }
  };
  return objectpath(reinterpret_cast<const void*>(&dummy::fn));
}

} // namespace vast::detail
