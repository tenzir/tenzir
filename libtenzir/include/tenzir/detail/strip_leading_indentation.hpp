//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>

namespace tenzir::detail {

/// Strips one level of leading indentation from a string, usually
/// representing a block of source code.
/// Looks for the first non-whitespace character in the string and
/// uses the character sequence from the beginning of the line to
/// that character as the indentation to strip.
/// @param code A newline-delimited multiline string.
auto strip_leading_indentation(std::string&& code) -> std::string;

} // namespace tenzir::detail
