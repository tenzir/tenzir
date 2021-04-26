//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/env.hpp"

#include "vast/error.hpp"

#include <fmt/format.h>

#include <cstdlib>
#include <mutex>

namespace vast::detail {

namespace {

// A mutex for locking calls to functions that mutate `environ`. Global to this
// translation unit.
auto env_mutex = std::mutex{};

} // namespace

std::optional<std::string> locked_getenv(const char* var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (const char* result = ::getenv(var))
    return result;
  return {};
}

caf::expected<void> locked_unsetenv(const char* var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (::unsetenv(var) == 0)
    return {};
  return caf::make_error( //
    ec::system_error,
    fmt::format("failed in unsetenv(3): {}", ::strerror(errno)));
}

} // namespace vast::detail
