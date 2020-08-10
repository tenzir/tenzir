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

#include "vast/detail/posix.hpp"
#include "vast/filesystem.hpp"

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

template <class Defaults>
caf::expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options) {
  std::string category = Defaults::category;
  auto output = get_or(options, category + ".write", Defaults::write);
  auto uds = get_or(options, category + ".uds", false);
  auto fifo = get_or(options, category + ".fifo", false);
  auto pt = uds ? path::socket : (fifo ? path::fifo : path::regular_file);
  return make_output_stream(output, pt);
}

caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, path::type pt = path::regular_file);

template <class Defaults>
caf::expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options) {
  std::string category = Defaults::category;
  auto input = get_or(options, category + ".read", Defaults::read);
  auto uds = get_or(options, category + ".uds", false);
  auto fifo = get_or(options, category + ".fifo", false);
  auto pt = uds ? path::socket : (fifo ? path::fifo : path::regular_file);
  return make_input_stream(input, pt);
}

} // namespace vast::detail
