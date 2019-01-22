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

#include <string>

#include <caf/expected.hpp>

#include "vast/fwd.hpp"
#include "vast/system/configuration.hpp"

namespace vast::system {

struct default_configuration : system::configuration {
  default_configuration(std::string application_name);
  caf::error parse(int argc, char** argv);
  caf::expected<path> setup_log_file(const path& base_dir);
  void merge_root_options(system::application& app);

  std::string application_name;
};

} // namespace vast::system
