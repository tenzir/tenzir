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

#include "vast/plugin.hpp"

#include <dlfcn.h>
#include <memory>

namespace vast {

plugin_ptr::plugin_ptr(const char* filename) {
  if (auto handle = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY)) {
    library_ = {handle, [](void* handle) noexcept { dlclose(handle); }};
    auto create_plugin = reinterpret_cast<plugin* (*) ()>(
      dlsym(library_.get(), "create_plugin"));
    auto destroy_plugin = reinterpret_cast<void (*)(plugin*)>(
      dlsym(library_.get(), "destroy_plugin"));
    if (create_plugin && destroy_plugin)
      if (auto plugin = create_plugin())
        instance_ = {plugin, destroy_plugin};
  }
}

} // namespace vast
