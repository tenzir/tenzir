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

#include "caf/fwd.hpp"

#include "vast/expected.hpp"

namespace vast::detail {

expected<std::unique_ptr<std::ostream>>
make_output_stream(const std::string& output, bool is_uds = false);

expected<std::unique_ptr<std::ostream>>
make_output_stream(const caf::config_value_map& options);

expected<std::unique_ptr<std::istream>>
make_input_stream(const std::string& input, bool is_uds = false);

expected<std::unique_ptr<std::istream>>
make_input_stream(const caf::config_value_map& options);

} // namespace vast::detail


