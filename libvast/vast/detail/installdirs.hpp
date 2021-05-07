//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>

namespace vast::detail {

/// Returns the configured data directory.
[[nodiscard]] std::filesystem::path install_datadir();

/// Returns the configured config directory.
[[nodiscard]] std::filesystem::path install_configdir();

/// Returns the configured plugin directory.
[[nodiscard]] std::filesystem::path install_plugindir();

} // namespace vast::detail
