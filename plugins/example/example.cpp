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
#include "vast/table_slice.hpp"

#include <caf/attach_stream_sink.hpp>
#include <caf/typed_event_based_actor.hpp>

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

  /// Construct a stream processor that hooks into the ingest path.
  stream_processor
  make_stream_processor(caf::actor_system& sys) const override {
    auto processor
      = [=](stream_processor::pointer self) -> stream_processor::behavior_type {
      return [=](caf::stream<table_slice> in) {
        VAST_WARNING(self, "hooks into stream");
        caf::attach_stream_sink(
          self, in,
          // Initialization hook for CAF stream.
          [=](uint64_t& counter) { // reset state
            VAST_WARNING(self, "initialized stream");
            counter = 0;
          },
          // Process one stream element at a time.
          [=](uint64_t& counter, table_slice slice) {
            counter += slice.rows();
            VAST_WARNING(self, "processed", counter, "cumulative events");
          },
          // Teardown hook for CAF stram.
          [=](uint64_t&, const caf::error& err) {
            if (err)
              VAST_ERROR(self, "finished with error", to_string(err));
          });
      };
    };
    return sys.spawn(processor);
  };
};

VAST_REGISTER_PLUGIN(example);
