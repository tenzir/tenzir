//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

#include <filesystem>

namespace vast::detail {

// Loads file contents into a string.
// @param p The path of the file to load.
// @returns The contents of the file *p*.
caf::expected<std::string> load_contents(const std::filesystem::path& p);

} // namespace vast::detail
