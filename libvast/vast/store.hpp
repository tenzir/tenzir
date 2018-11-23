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

#include <caf/fwd.hpp>

#include <caf/expected.hpp>

#include "vast/fwd.hpp"

namespace vast {

/// A key-value store for events.
class store {
public:
  virtual ~store();

  /// Adds a table slice to the store.
  /// @param xs The table slice to add.
  /// @returns No error on success.
  virtual caf::error put(table_slice_ptr xs) = 0;

  /// Retrieves a set of events.
  /// @param xs The IDs for the events to retrieve.
  /// @returns The table slice according to *xs*.
  virtual caf::expected<std::vector<table_slice_ptr>>
  get(const ids& xs) = 0;

  /// Flushes in-memory state to persistent storage.
  /// @returns No error on success.
  virtual caf::error flush() = 0;

  /// Fills `dict` with implementation-specific status information.
  virtual void inspect_status(caf::dictionary<caf::config_value>& dict) = 0;
};

} // namespace vast
