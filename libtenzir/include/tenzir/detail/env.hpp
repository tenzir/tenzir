//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/generator.hpp"

#include <caf/error.hpp>

#include <optional>
#include <string>
#include <string_view>

namespace tenzir::detail {

/// A thread-safe wrapper around `::getenv`.
/// @param var The environment variable.
/// @returns The copied environment variables contents, or `std::nullopt`.
[[nodiscard]] std::optional<std::string> getenv(std::string_view var);

/// A thread-safe wrapper around `::setenv`.
/// @param key The environment variable key.
/// @param value The environment variable value.
/// @param overwrite Flag to control whether existing keys get overwritten.
/// @returns The copied environment variables contents, or `std::nullopt`.
[[nodiscard]] caf::error
setenv(std::string_view key, std::string_view value, int overwrite = 1);

/// A thread-safe wrapper around `::unsetenv`.
/// @param var The environment variable.
/// @returns True on success.
[[nodiscard]] caf::error unsetenv(std::string_view var);

/// Retrieves all environment variables as list of key-value pairs.
/// The function processes the global variable `environ` that holds an array of
/// strings, each of which has the form `key=value` by convention (per `man
/// environ`). The results does not include variables that violate this
/// convention.
///
/// The returned `string_view`s must are only valid until the returned generator
/// is destroyed.
generator<std::pair<std::string_view, std::string_view>> environment();

} // namespace tenzir::detail
