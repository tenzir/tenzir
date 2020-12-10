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
#include "vast/time.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// The ACCOUNTANT actor interface.
using accountant_actor = caf::typed_actor<
  // FIXME: docs
  caf::replies_to<atom::config, accountant_config>::with< //
    atom::ok>,
  // FIXME: docs
  caf::reacts_to<atom::announce, std::string>,
  // FIXME: docs
  caf::reacts_to<std::string, duration>,
  // FIXME: docs
  caf::reacts_to<std::string, time>,
  // FIXME: docs
  caf::reacts_to<std::string, int64_t>,
  // FIXME: docs
  caf::reacts_to<std::string, uint64_t>,
  // FIXME: docs
  caf::reacts_to<std::string, double>,
  // FIXME: docs
  caf::reacts_to<report>,
  // FIXME: docs
  caf::reacts_to<performance_report>,
  // FIXME: docs
  caf::replies_to<atom::status, status_verbosity>::with< //
    caf::dictionary<caf::config_value>>,
  // FIXME: docs
  caf::reacts_to<atom::telemetry>>;

} // namespace vast::system
