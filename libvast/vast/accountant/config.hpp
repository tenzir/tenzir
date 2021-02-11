/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
    int64_t slice_size = 100;
    table_slice_encoding slice_type = defaults::import::table_slice_type;
  };

  struct file_sink {
    bool enable = false;
    std::string path;
  };

  struct uds_sink {
    bool enable = false;
    std::string path;
    detail::socket_type type;
  };

  self_sink self_sink;
  file_sink file_sink;
  uds_sink uds_sink;
  bool real_time = false;
};

caf::expected<accountant_config>
to_accountant_config(const caf::settings& opts);

} // namespace vast::system
