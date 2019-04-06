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

#include <iostream>
#include <memory>
#include <string>

#include "caf/settings.hpp"

#include "vast/expected.hpp"

namespace vast::detail {

expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, bool is_uds = false);

template <class Defaults>
expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::settings& options) {
  std::string category = Defaults::category;
  auto output = get_or(options, category + ".write", Defaults::write);
  auto uds = get_or(options, category + ".uds", false);
  return make_output_stream(output, uds);
}

expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, bool is_uds = false);

template <class Defaults>
expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::settings& options) {
  std::string category = Defaults::category;
  auto input = get_or(options, category + ".read", Defaults::read);
  auto uds = get_or(options, category + ".uds", false);
  return make_input_stream(input, uds);
}

} // namespace vast::detail
