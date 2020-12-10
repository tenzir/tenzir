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
#include "vast/system/accountant_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The TYPE REGISTRY actor interface.
using type_registry_actor = caf::typed_actor<
  // FIXME: docs
  caf::reacts_to<atom::telemetry>,
  // FIXME: docs
  caf::replies_to<atom::status, status_verbosity>::with< //
    caf::dictionary<caf::config_value>>,
  // FIXME: docs
  caf::reacts_to<caf::stream<table_slice>>,
  // FIXME: docs
  caf::reacts_to<atom::put, vast::type>,
  // FIXME: docs
  caf::reacts_to<atom::put, vast::schema>,
  // FIXME: docs
  caf::replies_to<atom::get>::with<type_set>,
  // FIXME: docs
  caf::replies_to<atom::get, atom::taxonomies>::with< //
    taxonomies>,
  // FIXME: docs
  caf::reacts_to<atom::put, taxonomies>,
  // FIXME: docs
  caf::replies_to<atom::load>::with< //
    atom::ok>,
  // FIXME: docs
  caf::replies_to<atom::resolve, expression>::with< //
    expression>,
  // FIXME: docs
  caf::reacts_to<accountant_actor>>;

} // namespace vast::system
