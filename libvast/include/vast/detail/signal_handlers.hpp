//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

/// A printing signal handler meant for SIGSEGV and SIGABRT. Prints a backtrace
/// if support for that is enabled at compile time.
extern "C" void fatal_handler(int sig);
