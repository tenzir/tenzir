//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/raise_error.hpp>

/// Throws an exception with string `msg` or terminates the application when
/// compiling exceptions disabled.
#define VAST_RAISE_ERROR(...) CAF_RAISE_ERROR(__VA_ARGS__)
