//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/string_literal.hpp"
#include "vast/test/test.hpp"

#include <fmt/format.h>

#include <fcntl.h>
#include <unistd.h>

namespace vast::test {

// Helper struct that, as long as it is alive, redirects stdin to the output of
// a file.
template <vast::detail::string_literal FileName = "">
struct stdin_file_input {
  stdin_file_input() {
    old_stdin_fd = ::dup(fileno(stdin));
    REQUIRE_NOT_EQUAL(old_stdin_fd, -1);
    auto input_file_fd = ::open(
      fmt::format("{}{}", VAST_TEST_PATH, FileName.str()).c_str(), O_RDONLY);
    REQUIRE_NOT_EQUAL(input_file_fd, -1);
    ::dup2(input_file_fd, fileno(stdin));
    ::close(input_file_fd);
  }

  ~stdin_file_input() {
    auto old_stdin_status = ::dup2(old_stdin_fd, fileno(stdin));
    REQUIRE_NOT_EQUAL(old_stdin_status, -1);
    ::close(old_stdin_fd);
  }
  int old_stdin_fd;
};

} // namespace vast::test
