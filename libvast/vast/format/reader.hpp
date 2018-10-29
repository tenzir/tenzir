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

#include <caf/expected.hpp>

#include "vast/fwd.hpp"

namespace vast::format {

/// The base class for readers.
class reader {
public:
  virtual ~reader();

  /// Reads the next event.
  /// @returns The event on success, `caf::none` if the underlying format has
  ///          currently no event (e.g., when it's idling), and an error
  ///          otherwise.
  virtual caf::expected<event> read() = 0;

  /// Sets the schema for events to read.
  /// @param x The new schema.
  /// @returns `caf::none` on success.
  virtual caf::expected<void> schema(vast::schema x) = 0;

  /// Retrieves the currently used schema.
  /// @returns The current schema.
  virtual caf::expected<vast::schema> schema() const = 0;

  /// @returns The name of the reader type.
  virtual const char* name() const = 0;
};

} // namespace vast::format
