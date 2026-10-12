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

// A `run` with `-r` or `--remote` runs on the platform, so in unified mode it
// goes to the platform CLI like every other command.
auto is_remote_run(int argc, char** argv) -> bool {
  for (auto i = 2; i < argc; ++i) {
    auto const argument = std::string_view{argv[i]};
    if (argument == "--") {
      return false;
    }
    if (argument == "-r" or argument == "--remote") {
      return true;
    }
  }
  return false;
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

auto run_platform_cli(std::filesystem::path const& libexec, std::string& name,
                      char** first, char** last) -> int {
  // The platform CLI starts deployments through the engine, so it needs to
  // know where to find it.
  if (::setenv("TENZIR_LIBEXEC", libexec.c_str(), 1) != 0) {
    std::cerr << "tenzir: failed to set TENZIR_LIBEXEC: "
              << std::strerror(errno) << '\n';
    return EXIT_FAILURE;
  }
  auto arguments = std::vector<char*>{name.data()};
  arguments.insert(arguments.end(), first, last);
  return run(libexec / "platform-cli", std::move(arguments));
}

} // namespace

auto main(int argc, char** argv) -> int {
  auto name = std::filesystem::path{argv[0]}.filename().string();
  auto const libexec
    = (executable_path(argv[0]).parent_path() / TENZIR_LIBEXEC_FROM_BINDIR)
        .lexically_normal();
  if (name == "tenzir") {
    auto const command
      = argc > 1 ? std::string_view{argv[1]} : std::string_view{};
    if (unified_mode()) {
      if (command != "run" or is_remote_run(argc, argv)) {
        return run_platform_cli(libexec, name, argv + 1, argv + argc);
      }
    } else if (command == "platform") {
      return run_platform_cli(libexec, name, argv + 2, argv + argc);
    }
  }
  auto arguments = std::vector<char*>{argv, argv + argc};
  arguments[0] = name.data();
  return run(libexec / "engine", std::move(arguments));
}
