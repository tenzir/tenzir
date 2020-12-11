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

#include "vast/aliases.hpp"
#include "vast/system/status_client_actor.hpp"
#include "vast/time.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// The ACCOUNTANT actor interface.
using accountant_actor = caf::typed_actor<
  // Update the configuration of the ACCOUNTANT.
  caf::replies_to<atom::config, accountant_config>::with< //
    atom::ok>,
  // Registers the sender with the ACCOUNTANT.
  caf::reacts_to<atom::announce, std::string>,
  // Record duration metric.
  caf::reacts_to<std::string, duration>,
  // Record time metric.
  caf::reacts_to<std::string, time>,
  // Record integer metric.
  caf::reacts_to<std::string, integer>,
  // Record count metric.
  caf::reacts_to<std::string, count>,
  // Record real metric.
  caf::reacts_to<std::string, real>,
  // Record a metrics report.
  caf::reacts_to<report>,
  // Record a performance report.
  caf::reacts_to<performance_report>,
  // The internal telemetry loop of the ACCOUNTANT.
  caf::reacts_to<atom::telemetry>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;

} // namespace vast::system
