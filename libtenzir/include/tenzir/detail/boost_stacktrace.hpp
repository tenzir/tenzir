//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"

#if TENZIR_USE_BOOST_STACKTRACE_LINK
#  define BOOST_STACKTRACE_LINK 1
#endif

#if TENZIR_USE_BOOST_STACKTRACE_BACKTRACE
#  define BOOST_STACKTRACE_USE_BACKTRACE 1
#endif

#define BOOST_STACKTRACE_GNU_SOURCE_NOT_REQUIRED 1

#include <boost/stacktrace.hpp>
