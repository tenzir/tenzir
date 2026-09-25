//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dbdir_size.hpp"

#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/detail/process.hpp"
#include "tenzir/detail/recursive_size.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <filesystem>

namespace tenzir {

caf::error validate(const disk_monitor_config& config) {
  if (config.step_size < 1) {
    return caf::make_error(ec::invalid_configuration, "step size must be "
                                                      "greater than zero");
  }
  if (config.low_water_mark > config.high_water_mark) {
    return caf::make_error(ec::invalid_configuration, "low-water mark greater "
                                                      "than high-water mark");
  }
  if (config.scan_binary) {
    if (config.scan_binary->empty()) {
      return caf::make_error(ec::invalid_configuration,
                             "scan binary path cannot be "
                             "empty");
    }
    if (config.scan_binary->at(0) != '/') {
      return caf::make_error(ec::invalid_configuration,
                             "scan binary path must be "
                             "an absolute");
    }
    if (not std::filesystem::exists(*config.scan_binary)) {
      return caf::make_error(ec::invalid_configuration, "scan binary doesn't "
                                                        "exist");
    }
  }
  return {};
}

namespace {

auto scan_files(const partition_paths& paths) -> caf::expected<disk_usage> {
  auto result = disk_usage{};
  auto size = detail::recursive_size(
    paths.database_dir, [&](const std::filesystem::path& path, size_t bytes) {
      const auto parent = path.parent_path();
      if (parent != paths.index_dir and parent != paths.synopsis_dir
          and parent != paths.archive_dir) {
        return;
      }
      auto id = uuid{};
      if (parsers::uuid(path.stem().string(), id)) {
        result.partition_bytes[id] += bytes;
      }
    });
  if (not size) {
    return size.error();
  }
  result.bytes = *size;
  return result;
}

} // namespace

caf::expected<disk_usage>
compute_dbdir_size(const partition_paths& paths,
                   const disk_monitor_config& config) {
  auto result = scan_files(paths);
  if (not result) {
    return result.error();
  }
  if (not config.scan_binary) {
    return result;
  }
  const auto& command
    = fmt::format("{} {}", *config.scan_binary, paths.database_dir);
  TENZIR_VERBOSE("executing command '{}' to determine size of state_directory",
                 command);
  auto cmd_output = detail::execute_blocking(command);
  if (not cmd_output) {
    return cmd_output.error();
  }
  if (not cmd_output->empty() and cmd_output->back() == '\n') {
    cmd_output->pop_back();
  }
  if (not parsers::count(*cmd_output, result->bytes)) {
    return caf::make_error(ec::parse_error,
                           fmt::format("failed to interpret output "
                                       "'{}' of command '{}'",
                                       *cmd_output, command));
  }
  auto after = scan_files(paths);
  if (not after) {
    return after.error();
  }
  result->stable = result->partition_bytes == after->partition_bytes;
  return result;
}

} // namespace tenzir
