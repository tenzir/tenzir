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

#include "vast/detail/assert.hpp"

#include <dlfcn.h>
#include <memory>
#include <tuple>

namespace vast {

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

std::vector<plugin_ptr>& get() noexcept {
  static auto plugins = std::vector<plugin_ptr>{};
  return plugins;
}

} // namespace plugins

// -- plugin version -----------------------------------------------------------

bool has_required_version(const plugin_version& version) noexcept {
  return std::tie(plugin::version.major, plugin::version.minor,
                  plugin::version.patch, plugin::version.tweak)
         <= std::tie(version.major, version.minor, version.patch,
                     version.tweak);
}

// -- plugin_ptr ---------------------------------------------------------------

plugin_ptr::plugin_ptr(const char* filename) noexcept {
  if (auto handle = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY)) {
    library_ = handle;
    auto plugin_version = reinterpret_cast<::vast::plugin_version (*)()>(
      dlsym(library_, "plugin_version"));
    if (plugin_version && has_required_version(plugin_version())) {
      auto plugin_create = reinterpret_cast<::vast::plugin* (*) ()>(
        dlsym(library_, "plugin_create"));
      auto plugin_destroy = reinterpret_cast<void (*)(::vast::plugin*)>(
        dlsym(library_, "plugin_destroy"));
      if (plugin_create && plugin_destroy) {
        instance_ = plugin_create();
        deleter_ = plugin_destroy;
      }
    }
  }
}

plugin_ptr::~plugin_ptr() noexcept {
  if (instance_) {
    VAST_ASSERT(library_);
    VAST_ASSERT(deleter_);
    deleter_(instance_);
    instance_ = {};
    deleter_ = {};
  }
  if (library_) {
    dlclose(library_);
    library_ = {};
  }
}

plugin_ptr::plugin_ptr(plugin_ptr&& other) noexcept
  : library_{std::exchange(other.library_, {})},
    instance_{std::exchange(other.instance_, {})},
    deleter_{std::exchange(other.deleter_, {})} {
  // nop
}

plugin_ptr& plugin_ptr::operator=(plugin_ptr&& rhs) noexcept {
  library_ = std::exchange(rhs.library_, {});
  instance_ = std::exchange(rhs.instance_, {});
  deleter_ = std::exchange(rhs.deleter_, {});
  return *this;
}

plugin_ptr::operator bool() noexcept {
  return static_cast<bool>(instance_);
}

const plugin* plugin_ptr::operator->() const noexcept {
  return instance_;
}

plugin* plugin_ptr::operator->() noexcept {
  return instance_;
}

const plugin& plugin_ptr::operator*() const noexcept {
  return *instance_;
}

plugin& plugin_ptr::operator&() noexcept {
  return *instance_;
}

} // namespace vast
