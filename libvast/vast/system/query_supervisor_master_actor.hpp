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

#include "vast/system/query_supervisor_actor.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// The QUERY SUPERVISOR MASTER actor interface.
using query_supervisor_master_actor = caf::typed_actor<
  // Enlist the QUERY SUPERVISOR as an available worker.
  caf::reacts_to<atom::worker, query_supervisor_actor>>;

} // namespace vast::system
