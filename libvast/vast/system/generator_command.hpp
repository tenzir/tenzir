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

#include <memory>
#include <random>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/system/reader_command_base.hpp"
#include "vast/system/source.hpp"

namespace vast::system {

/// Default implementation for import sub-commands. Compatible with Bro and MRT
/// formats.
/// @relates application
template <class Generator>
class generator_command : public reader_command_base {
public:
  generator_command(command* parent, std::string_view name)
      : reader_command_base(parent, name) {
    add_opt<size_t>("seed", "the random seed");
    add_opt<size_t>("num,N", "events to generate");
  }

protected:
  expected<caf::actor>
  make_source(caf::scoped_actor& self,
              const caf::config_value_map& options) override {
    VAST_TRACE("");
    auto seed = caf::get_if<size_t>(&options, "seed");
    if (!seed)
      seed = {std::random_device{}()};
    auto num = get_or(options, "num", defaults::command::generated_events);
    Generator generator{*seed, num};
    return self->spawn(default_source<Generator>, std::move(generator));
  }
};

} // namespace vast::system
