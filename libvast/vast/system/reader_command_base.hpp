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

#include <string_view>

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include "vast/system/node_command.hpp"

namespace vast::system {

/// Format-independent implementation for import sub-commands.
class reader_command_base : public node_command {
public:
  using super = node_command;

  reader_command_base(command* parent, std::string_view name);

protected:
  int run_impl(caf::actor_system& sys, const caf::config_value_map& options,
               argument_iterator begin, argument_iterator end) override;

  virtual expected<caf::actor> make_source(
    caf::scoped_actor& self,
    const caf::config_value_map& options) = 0;
};

} // namespace vast::system
