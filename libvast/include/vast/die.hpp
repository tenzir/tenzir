//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>

namespace vast {

/// Terminates the process immediately.
/// @param msg The message to print before terminating.
[[noreturn]] void die(const std::string& = "");

} // namespace vast
