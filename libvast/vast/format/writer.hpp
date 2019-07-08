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

#include <caf/expected.hpp>

#include "vast/fwd.hpp"

namespace vast::format {

/// The base class for writers.
class writer {
public:
  virtual ~writer();

  /// Processes an event.
  /// @param x The event to write.
  /// @returns `caf::unit` on success.
  /// @note Legacy API! This exists only for backwards compatibility at the
  ///       moment and is going to get removed when all implementations of this
  ///       interface switched to the `table_slice` API.
  virtual caf::expected<void> write(const event& x);

  /// Processes a batch of events.
  /// @param xs The events to write.
  /// @returns `caf::none` on success.
  /// @note Legacy API! This exists only for backwards compatibility at the
  ///       moment and is going to get removed when all implementations of this
  ///       interface switched to the `table_slice` API.
  virtual caf::error write(const std::vector<event>& xs);

  /// Processes a single batch of events.
  /// @param x The events to write wrapped in a table slice.
  /// @returns `caf::none` on success.
  virtual caf::error write(const table_slice& x);

  /// Called periodically to flush state.
  /// @returns `caf::none` on success.
  /// The default implementation does nothing.
  virtual caf::expected<void> flush();

  /// @returns The name of the writer type.
  virtual const char* name() const = 0;
};

} // namespace vast::format
