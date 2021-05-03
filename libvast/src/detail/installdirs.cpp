//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/installdirs.hpp"

#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/process.hpp"

namespace vast::detail {

#if VAST_ENABLE_RELOCATABLE_INSTALLATIONS

namespace {

std::filesystem::path install_prefix() {
  const auto binary = detail::objectpath(nullptr);
  VAST_ASSERT(binary);
  return binary->parent_path().parent_path();
}

} // namespace

#endif

std::filesystem::path install_datadir() {
#if VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  return install_prefix() / VAST_DATADIR;
#else
  return VAST_FULL_DATADIR;
#endif
}

std::filesystem::path install_configdir() {
#if VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  return install_prefix() / VAST_CONFIGDIR;
#else
  return VAST_FULL_CONFIGDIR;
#endif
}

std::filesystem::path install_plugindir() {
#if VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  return install_prefix() / VAST_PLUGINDIR;
#else
  return VAST_FULL_PLUGINDIR;
#endif
}

} // namespace vast::detail
