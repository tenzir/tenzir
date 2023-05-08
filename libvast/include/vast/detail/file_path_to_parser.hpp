//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>

namespace vast::detail {

/// Function to return a parser for a file path to be used as loader operator
/// input in a pipeline. Currently only strips away the extension and returns
/// that stripped extension as a parser.
/// @param path The path to determine the parser for.
/// @returns A string determining the default parser for a file.
auto file_path_to_parser(const std::filesystem::path& path) -> std::string;

} // namespace vast::detail
