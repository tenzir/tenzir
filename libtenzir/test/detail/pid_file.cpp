//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/config.hpp>
#include <tenzir/detail/pid_file.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/uuid.hpp>

#include <fmt/std.h>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <unistd.h>

namespace tenzir {
namespace {

TEST("PID file reads preserve empty input and textual diagnostics") {
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tenzir-pid-{}", uuid::random());
  auto cleanup = detail::scope_guard{[&]() noexcept {
    auto error = std::error_code{};
    std::filesystem::remove(path, error);
  }};
  auto pid = std::to_string(::getpid());
  for (auto const& content : {std::string{}, pid}) {
    auto output = std::ofstream{path};
    output << content;
    output.close();
    REQUIRE(static_cast<bool>(output));
    CHECK(not detail::acquire_pid_file(path));
    auto input = std::ifstream{path};
    auto observed = std::string{std::istreambuf_iterator<char>{input}, {}};
    CHECK_EQUAL(observed, pid);
  }
#if not TENZIR_LINUX
  // Linux deliberately exempts non-Tenzir processes. On other platforms, the
  // live parent provides a conflict without spawning or timing another task.
  auto parent = std::to_string(::getppid());
  auto output = std::ofstream{path};
  output << parent;
  output.close();
  REQUIRE(static_cast<bool>(output));
  auto error = detail::acquire_pid_file(path);
  REQUIRE(error);
  REQUIRE(error.context().match_elements<std::string>());
  CHECK_EQUAL(error.context().get_as<std::string>(0),
              fmt::format("PID file found: {}, terminate process {}", path,
                          parent));
#endif
}

} // namespace
} // namespace tenzir
