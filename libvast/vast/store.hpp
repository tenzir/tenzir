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

#include <vector>

#include "vast/expected.hpp"
#include "vast/ids.hpp"

namespace vast {

class event;

// TODO: Instead of using vector<event> as an abstraction for a set of events,
// we'd like to use vast::batch here eventually. However, this requires first
// an update of the batch interface/semantics, which is part of a separate
// story.

/// A key-value store for events.
class store {
public:
  virtual ~store() = default;

  /// Stores a set of events.
  /// @param xs The events to store.
  /// @returns No error on success.
  virtual expected<void> put(const std::vector<event>& xs) = 0;

  /// Retrieves a set of events.
  /// @param xs The IDs for the events to retrieve.
  /// @returns The events according to *xs*.
  virtual expected<std::vector<event>> get(const ids& xs) = 0;

  /// Flushes in-memory state to persistent storage.
  /// @returns No error on success.
  virtual expected<void> flush() = 0;
};

} // namespace vast

