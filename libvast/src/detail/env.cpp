//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/env.hpp"

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

#include <fmt/format.h>

#include <cstdio>
#include <cstdlib>
#include <mutex>

extern char** environ;

namespace vast::detail {

namespace {

// A mutex for locking calls to functions that mutate `environ`. Global to this
// translation unit.
auto env_mutex = std::mutex{};

} // namespace

std::optional<std::string_view> getenv(std::string_view var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (const char* result = ::getenv(var.data()))
    return std::string_view{result};
  return {};
}

caf::error setenv(std::string_view key, std::string_view value, int overwrite) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (::setenv(key.data(), value.data(), overwrite) == 0)
    return {};
  return caf::make_error( //
    ec::system_error,
    fmt::format("failed in unsetenv(3): {}", ::strerror(errno)));
}

caf::error unsetenv(std::string_view var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (::unsetenv(var.data()) == 0)
    return {};
  return caf::make_error( //
    ec::system_error,
    fmt::format("failed in unsetenv(3): {}", ::strerror(errno)));
}

generator<std::pair<std::string_view, std::string_view>> environment() {
  // Envrionment variables come as "key=value" pair strings.
  for (auto env = environ; *env != nullptr; ++env) {
    auto str = std::string_view{*env};
    auto i = str.find('=');
    VAST_ASSERT(i != std::string::npos);
    auto key = str.substr(0, i);
    auto value = str.substr(i + 1);
    co_yield std::pair{key, value};
  }
}

} // namespace vast::detail
