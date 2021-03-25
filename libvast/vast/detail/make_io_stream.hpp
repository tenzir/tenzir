//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/defaults.hpp"
#include "vast/detail/posix.hpp"

#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

namespace vast::detail {

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, socket_type st);

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output,
                   std::filesystem::file_type file_type
                   = std::filesystem::file_type::regular);

inline caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options) {
  auto output = get_or(options, "vast.export.write", defaults::export_::write);
  auto uds = get_or(options, "vast.export.uds", false);
  auto fifo = get_or(options, "vast.export.fifo", false);
  const auto pt = uds ? std::filesystem::file_type::socket
                      : (fifo ? std::filesystem::file_type::fifo
                              : std::filesystem::file_type::regular);
  return make_output_stream(output, pt);
}

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input,
                  std::filesystem::file_type file_type
                  = std::filesystem::file_type::regular);

inline caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options) {
  auto input = get_or(options, "vast.import.read", defaults::import::read);
  auto uds = get_or(options, "vast.import.uds", false);
  auto fifo = get_or(options, "vast.import.fifo", false);
  const auto pt = uds ? std::filesystem::file_type::socket
                      : (fifo ? std::filesystem::file_type::fifo
                              : std::filesystem::file_type::regular);
  return make_input_stream(input, pt);
}

} // namespace vast::detail
