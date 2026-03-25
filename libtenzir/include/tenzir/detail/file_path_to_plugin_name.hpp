//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>
#include <optional>

namespace tenzir::detail {

/// Function to return a format plugin name for a file path to be used as loader
/// operator input in a pipeline. Currently only strips away the extension and
/// returns that stripped extension as a plugin name.
/// @param path The path to determine the format plugin name for.
/// @returns A string determining the format default plugin name for a file.
auto file_path_to_plugin_name(const std::filesystem::path& path)
  -> std::optional<std::string>;

} // namespace tenzir::detail
