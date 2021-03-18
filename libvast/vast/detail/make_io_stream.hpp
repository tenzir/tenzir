// SPDX-FileCopyrightText: (c) 2017 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/defaults.hpp"
#include "vast/detail/posix.hpp"
#include "vast/path.hpp"

#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <iostream>
#include <memory>
#include <string>

namespace vast::detail {

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, socket_type st);

caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, path::type pt
                                              = path::regular_file);

inline caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options) {
  auto output = get_or(options, "vast.export.write", defaults::export_::write);
  auto uds = get_or(options, "vast.export.uds", false);
  auto fifo = get_or(options, "vast.export.fifo", false);
  auto pt = uds ? path::socket : (fifo ? path::fifo : path::regular_file);
  return make_output_stream(output, pt);
}

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, path::type pt = path::regular_file);

inline caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options) {
  auto input = get_or(options, "vast.import.read", defaults::import::read);
  auto uds = get_or(options, "vast.import.uds", false);
  auto fifo = get_or(options, "vast.import.fifo", false);
  auto pt = uds ? path::socket : (fifo ? path::fifo : path::regular_file);
  return make_input_stream(input, pt);
}

} // namespace vast::detail
