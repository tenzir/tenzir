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

#ifndef VAST_SYSTEM_RUN_IMPORT_HPP
#define VAST_SYSTEM_RUN_IMPORT_HPP

#include <memory>
#include <string>
#include <string_view>

#include "vast/system/base_command.hpp"

namespace vast::system {

/// Default implementation for the `import` command.
/// @relates application
class run_import : public base_command {
public:
  run_import(command* parent, std::string_view name);

protected:
  int run_impl(caf::actor_system& sys, option_map& options,
               caf::message args) override;
};

} // namespace vast::system

#endif
