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
#include "vast/error.hpp"

#include <caf/expected.hpp>

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

std::string to_string(plugin_version x) {
  using std::to_string;
  std::string result;
  result += to_string(x.major);
  result += '.';
  result += to_string(x.major);
  result += '.';
  result += to_string(x.patch);
  result += '-';
  result += to_string(x.tweak);
  return result;
}

bool has_required_version(const plugin_version& version) noexcept {
  return plugin::version.major == version.major
         && std::tie(plugin::version.minor, plugin::version.patch,
                     plugin::version.tweak)
              <= std::tie(version.minor, version.patch, version.tweak);
}

// -- plugin_ptr ---------------------------------------------------------------

caf::expected<plugin_ptr> plugin_ptr::make(const char* filename) noexcept {
  auto library = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY);
  if (!library)
    return caf::make_error(ec::system_error, "failed to load plugin", filename,
                           dlerror());
  auto libvast_version = reinterpret_cast<const char* (*) ()>(
    dlsym(library, "vast_libvast_version"));
  if (!libvast_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_libvast_version in",
                           filename, dlerror());
  if (strcmp(libvast_version(), VAST_VERSION))
    return caf::make_error(ec::version_error, "libvast version mismatch in",
                           filename, libvast_version(), VAST_VERSION);
  auto libvast_build_tree_hash = reinterpret_cast<const char* (*) ()>(
    dlsym(library, "vast_libvast_build_tree_hash"));
  if (!libvast_build_tree_hash)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "vast_libvast_build_tree_hash in",
                           filename, dlerror());
  if (strcmp(libvast_build_tree_hash(), VAST_BUILD_TREE_HASH))
    return caf::make_error(ec::version_error,
                           "libvast build tree hash mismatch in", filename,
                           libvast_build_tree_hash(), VAST_BUILD_TREE_HASH);
  auto plugin_version = reinterpret_cast<::vast::plugin_version (*)()>(
    dlsym(library, "vast_plugin_version"));
  if (!plugin_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_version in",
                           filename, dlerror());
  if (!has_required_version(plugin_version()))
    return caf::make_error(ec::version_error, "plugin version mismatch",
                           filename, plugin_version(), plugin::version);
  auto plugin_create = reinterpret_cast<::vast::plugin* (*) ()>(
    dlsym(library, "vast_plugin_create"));
  if (!plugin_create)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_create in",
                           filename, dlerror());
  auto plugin_destroy = reinterpret_cast<void (*)(::vast::plugin*)>(
    dlsym(library, "vast_plugin_destroy"));
  if (!plugin_destroy)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_destroy in",
                           filename, dlerror());
  return plugin_ptr{library, plugin_create(), plugin_destroy};
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

plugin_ptr::plugin_ptr(void* library, plugin* instance,
                       void (*deleter)(plugin*)) noexcept
  : library_{library}, instance_{instance}, deleter_{deleter} {
  // nop
}

plugin_version plugin_ptr::version() const {
  auto plugin_version = reinterpret_cast<::vast::plugin_version (*)()>(
    dlsym(library_, "vast_plugin_version"));
  VAST_ASSERT(plugin_version);
  return plugin_version();
}

} // namespace vast
