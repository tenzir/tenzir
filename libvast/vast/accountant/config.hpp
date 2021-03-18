// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/defaults.hpp"
#include "vast/detail/posix.hpp"

#include <caf/expected.hpp>

#include <string>

namespace vast::system {

struct accountant_config {
  struct self_sink {
    bool enable = true;
    // TODO: Switch to unsigned when moving to vast::record for transmitting.
    int64_t slice_size = 128;
    table_slice_encoding slice_type = defaults::import::table_slice_type;
  };

  struct file_sink {
    bool enable = false;
    bool real_time = false;
    std::string path;
  };

  struct uds_sink {
    bool enable = false;
    bool real_time = false;
    std::string path;
    detail::socket_type type;
  };

  self_sink self_sink;
  file_sink file_sink;
  uds_sink uds_sink;
};

caf::expected<accountant_config>
to_accountant_config(const caf::settings& opts);

} // namespace vast::system
