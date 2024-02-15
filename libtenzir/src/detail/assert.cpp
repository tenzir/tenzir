//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include "tenzir/detail/backtrace.hpp"
#include "tenzir/logger.hpp"

namespace tenzir::detail {

void panic(std::string message) {
  // TODO: Ideally, we would capture a stacktrace here and print it after some
  // post-processing, potentially only after being caught. The exception type
  // should be change to a special panic exception.
  TENZIR_ERROR("PANIC: {}", message);
  backtrace();
  throw std::runtime_error(message);
}

[[noreturn]] void
fail_assertion_impl(const char* expr, std::string_view explanation,
                    std::source_location source) {
  auto message = fmt::format("assertion `{}` failed", expr);
  if (not explanation.empty()) {
    message += ": ";
    message += explanation;
  }
  message += fmt::format(" @ {}:{}", source.file_name(), source.line());
  panic(std::move(message));
}

} // namespace tenzir::detail
