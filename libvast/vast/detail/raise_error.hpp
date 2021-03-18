// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/raise_error.hpp>

/// Throws an exception with string `msg` or terminates the application when
/// compiling exceptions disabled.
#define VAST_RAISE_ERROR(...) CAF_RAISE_ERROR(__VA_ARGS__)

