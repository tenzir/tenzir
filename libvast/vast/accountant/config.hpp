//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
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

  template <class Inspector>
  friend auto inspect(Inspector& f, accountant_config& x) {
    f(caf::meta::type_name("vast::system::accountant_config"),
      x.self_sink.enable, x.self_sink.slice_size, x.file_sink.enable,
      x.file_sink.path, x.uds_sink.enable, x.uds_sink.path);
  }
};

caf::expected<accountant_config>
to_accountant_config(const caf::settings& opts);

} // namespace vast::system
