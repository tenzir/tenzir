//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>

namespace tenzir::detail {

/// Returns the configured config directory.
[[nodiscard]] std::filesystem::path install_configdir();

/// Returns the configured data directory.
[[nodiscard]] std::filesystem::path install_datadir();

/// Returns the configured libexec directory.
[[nodiscard]] std::filesystem::path install_libexecdir();

/// Returns the configured plugin directory.
[[nodiscard]] std::filesystem::path install_plugindir();

} // namespace tenzir::detail
