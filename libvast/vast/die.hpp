// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <string>

namespace vast {

/// Terminates the process immediately.
/// @param msg The message to print before terminating.
[[noreturn]] void die(const std::string& = "");

} // namespace vast

