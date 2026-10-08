//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

auto unified_mode() -> bool {
  auto const* value = std::getenv("TENZIR_UNIFIED");
  if (not value or not *value) {
    return false;
  }
  auto normalized = std::string{value};
  for (auto& c : normalized) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return normalized != "0" and normalized != "false" and normalized != "no";
}

auto executable_path(std::string_view argv0) -> std::filesystem::path {
  auto path = std::filesystem::path{argv0};
  if (not path.has_parent_path()) {
    if (auto const* environment_path = std::getenv("PATH")) {
      for (auto rest = std::string_view{environment_path};;) {
        auto const separator = rest.find(':');
        auto const directory = rest.substr(0, separator);
        auto candidate
          = std::filesystem::path{directory.empty() ? "." : directory} / path;
        if (::access(candidate.c_str(), X_OK) == 0) {
          path = std::move(candidate);
          break;
        }
        if (separator == std::string_view::npos) {
          break;
        }
        rest.remove_prefix(separator + 1);
      }
    }
  }
  auto error = std::error_code{};
  auto result = std::filesystem::canonical(path, error);
  if (not error) {
    return result;
  }
  result = std::filesystem::absolute(path, error);
  return error ? path : result;
}

auto run(std::filesystem::path const& binary, std::vector<char*> arguments)
  -> int {
  arguments.push_back(nullptr);
  ::execv(binary.c_str(), arguments.data());
  std::cerr << "tenzir: failed to execute " << binary << ": "
            << std::strerror(errno) << '\n';
  return EXIT_FAILURE;
}

} // namespace

auto main(int argc, char** argv) -> int {
  auto name = std::filesystem::path{argv[0]}.filename().string();
  auto const libexec
    = executable_path(argv[0]).parent_path() / TENZIR_LIBEXEC_FROM_BINDIR;
  if (name == "tenzir" and unified_mode()) {
    auto const command
      = argc > 1 ? std::string_view{argv[1]} : std::string_view{};
    if (command == "up" or command == "run") {
      name = command == "up" ? "tenzir-up" : "tenzir";
      auto arguments = std::vector<char*>{name.data()};
      arguments.insert(arguments.end(), argv + 2, argv + argc);
      return run(libexec / "engine", std::move(arguments));
    }
    return run(libexec / "platform-cli", {argv, argv + argc});
  }
  if (name == "tenzir" and argc > 1
      and std::string_view{argv[1]} == "platform") {
    auto arguments = std::vector<char*>{argv[0]};
    arguments.insert(arguments.end(), argv + 2, argv + argc);
    return run(libexec / "platform-cli", std::move(arguments));
  }
  auto arguments = std::vector<char*>{argv, argv + argc};
  arguments[0] = name.data();
  return run(libexec / "engine", std::move(arguments));
}
