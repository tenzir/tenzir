//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/env.hpp"

#include <cstdlib>
#include <mutex>

namespace vast::detail {

namespace {

auto env_mutex = std::mutex{};

} // namespace

std::optional<std::string> env(const char* var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  if (const char* result = ::getenv(var))
    return result;
  return {};
}

bool unset_env(const char* var) {
  auto lock = std::scoped_lock{env_mutex};
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  return ::unsetenv(var) == 0;
}

} // namespace vast::detail
