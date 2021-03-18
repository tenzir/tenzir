// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

namespace vast::detail {

/// Prints a stack backtrace on stderr.
// Tries to use the following mechanisms in order:
// 1. execinfo.h (via glibc or libexecinfo)
// 2. libunwind
// 3. libbacktrace
void backtrace();

} // namespace vast::detail
