//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <boost/stacktrace.hpp>

namespace tenzir::detail {

auto format_frame(const boost::stacktrace::frame& frame) -> std::string;

auto has_async_stacktrace() -> bool;

void print_async_stacktrace(std::ostream& out);

} // namespace tenzir::detail
