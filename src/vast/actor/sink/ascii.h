#ifndef VAST_ACTOR_SINK_ASCII_H
#define VAST_ACTOR_SINK_ASCII_H

#include "vast/actor/sink/base.h"
#include "vast/actor/sink/stream.h"

namespace vast {
namespace sink {

/// A sink dumping events in plain ASCII.
class ascii : public base<ascii>
{
public:
  /// Spawns an ASCII sink.
  /// @param p The output path.
  ascii(path p);

  bool process(event const& e);

private:
  path dir_;
  stream stream_;
};

} // namespace sink
} // namespace vast

#endif

