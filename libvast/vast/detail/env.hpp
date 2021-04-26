//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/expected.hpp>

#include <optional>
#include <string>

namespace vast::detail {

/// A thread-safe wrapper around `::getenv`.
/// @param var The environment variable.
/// @returns The copied environment variables contents, or `std::nullopt`.
[[nodiscard]] std::optional<std::string> locked_getenv(const char* var);

/// A thread-safe wrapper around `::unsetenv`.
/// @param var The environment variable.
/// @returns True on success.
[[nodiscard]] caf::expected<void> locked_unsetenv(const char* var);

} // namespace vast::detail
