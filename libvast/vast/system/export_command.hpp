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

#ifndef VAST_SYSTEM_RUN_EXPORT_HPP
#define VAST_SYSTEM_RUN_EXPORT_HPP

#include <memory>
#include <string>
#include <string_view>

#include "vast/system/base_command.hpp"

namespace vast::system {

/// Default implementation for the `export` command.
/// @relates application
class export_command : public base_command {
public:
  export_command(command* parent, std::string_view name);

protected:
  int run_impl(caf::actor_system& sys, option_map& options,
               caf::message args) override;
};

} // namespace vast::system

#endif
