#ifndef VAST_ACTOR_SINK_JSON_H
#define VAST_ACTOR_SINK_JSON_H

#include <iosfwd>
#include <memory>

#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

struct json_state : state {
  json_state(local_actor *self);
  ~json_state();

  bool process(event const& e) override;

  void flush() override;

  std::unique_ptr<std::ostream> out;
  bool first = true;
  bool flatten = false;
};

/// A sink dumping events in JSON.
/// @param self The actor handle.
/// @param out The stream to print received events into.
/// @param flatten Flag indicating whether to flatten records.
behavior json(stateful_actor<json_state>* self,
              std::unique_ptr<std::ostream> out, bool flatten);

} // namespace sink
} // namespace vast

#endif
