//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::detail {

/// Prints a stack backtrace on stderr.
/// @note Tries to use the following mechanisms in order: libunwind,
/// libbacktrace, execinfo.h.
void backtrace();

} // namespace vast::detail
