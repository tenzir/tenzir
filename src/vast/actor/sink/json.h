#ifndef VAST_ACTOR_SINK_JSON_H
#define VAST_ACTOR_SINK_JSON_H

#include "vast/actor/sink/base.h"
#include "vast/actor/sink/stream.h"

namespace vast {
namespace sink {

/// A sink generating JSON output.
class json : public base<json>
{
public:
  /// Spawns a JSON sink.
  /// @param p The output path.
  json(path p);

  bool process(event const& e);

private:
  path dir_;
  stream stream_;
};

} // namespace sink
} // namespace vast

#endif
