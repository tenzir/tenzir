//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/dbdir_size.hpp"

#include "tenzir/concept/parseable/tenzir/si.hpp"
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

caf::expected<size_t> compute_dbdir_size(std::filesystem::path state_directory,
                                         const disk_monitor_config& config) {
  caf::expected<size_t> result = 0;
  if (not config.scan_binary) {
    return detail::recursive_size(state_directory);
  }
  const auto& command
    = fmt::format("{} {}", *config.scan_binary, state_directory);
  TENZIR_VERBOSE("executing command '{}' to determine size of state_directory",
                 command);
  auto cmd_output = detail::execute_blocking(command);
  if (not cmd_output) {
    return cmd_output.error();
  }
  if (cmd_output->back() == '\n') {
    cmd_output->pop_back();
  }
  if (not parsers::count(*cmd_output, result.value())) {
    result = caf::make_error(ec::parse_error,
                             fmt::format("failed to interpret output "
                                         "'{}' of command '{}'",
                                         *cmd_output, command));
  }
  return result;
}

} // namespace tenzir
