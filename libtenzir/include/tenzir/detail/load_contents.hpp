//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

#include <filesystem>

namespace tenzir::detail {

// Loads file contents into a string.
// @param p The path of the file to load.
// @returns The contents of the file *p*.
caf::expected<std::string> load_contents(const std::filesystem::path& p);

} // namespace tenzir::detail
