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

#include <caf/optional.hpp>

#include "vast/schema.hpp"

namespace vast {

class event_types {
public:
  /// Initializes the system-wide type registry.
  /// @param s The schema.
  /// @returns true on success or false if registry was alread initialized.
  static bool init(schema s);

  /// Retrieves a pointer to the system-wide type registry.
  /// @returns nullptr if registry is not initialized.
  static const schema* get();

private:
  static schema& get_impl();
  static bool initialized;
};

} // namespace vast
