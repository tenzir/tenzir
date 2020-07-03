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

#include "vast/fwd.hpp"

namespace vast::system {

/// Spawns a sink based on the output format name.
/// @param sys The actor system to spawn the sink in.
/// @param options The invocation options for configuring writer and sink.
/// @param output_format A valid output format name.
caf::expected<caf::actor>
make_sink(caf::actor_system& sys, const caf::settings& options,
          std::string output_format);

} // namespace vast::system
