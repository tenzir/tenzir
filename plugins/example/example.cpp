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

#include "vast/data.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"

// FIXME: hackathon yolo mode
using namespace vast;

/// An example plugin.
class example : public plugin {
public:
  /// Teardown logic.
  ~example() override {
    VAST_WARNING_ANON("tearing down example plugin");
  }

  /// Process YAML configuration.
  caf::error initialize(data /* config */) override {
    VAST_WARNING_ANON("initalizing example plugin");
    return caf::none;
  }

  /// Unique name of the plugin.
  const char* name() const override {
    return "example";
  }
};

plugin_ptr plugin::make() {
  return plugin_ptr{new example};
}
