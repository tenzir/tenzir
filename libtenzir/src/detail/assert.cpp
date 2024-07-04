//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/backtrace.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/logger.hpp"

#include <cstdlib>

namespace tenzir::detail {

void panic_impl(std::string message, std::source_location source) {
  // TODO: Ideally, we would capture a stacktrace here and print it after some
  // post-processing, potentially only after being caught. The exception type
  // should be change to a special panic exception.
  backtrace();
  TENZIR_ERROR("panic: {}", message);
  TENZIR_ERROR("version: {}", version::version);
  TENZIR_ERROR("source: {}:{}", source.file_name(), source.line());
  TENZIR_ERROR("this is a bug, we would appreciate a report - thank you!");
  // TODO: Consider making this available through the normal configuration
  // mechanism.
  if (auto e = detail::getenv("TENZIR_ABORT_ON_PANIC");
      e && not e->empty() && *e != "0") {
    // Wait until `spdlog` flushed the logs.
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
    std::_Exit(1);
  }
  message += fmt::format(" @ {}:{}", source.file_name(), source.line());
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
  panic_impl(std::move(message), source);
}

} // namespace tenzir::detail
