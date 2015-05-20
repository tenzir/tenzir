#ifndef VAST_ACTOR_SINK_ASCII_H
#define VAST_ACTOR_SINK_ASCII_H

#include "vast/actor/sink/base.h"

namespace vast {

namespace io { class output_stream; }

namespace sink {

/// A sink dumping events in plain ASCII.
class ascii : public base<ascii>
{
public:
  /// Spawns an ASCII sink.
  /// @param out The output stream.
  ascii(std::unique_ptr<io::output_stream> out);

  bool process(event const& e);

  void flush();

private:
  std::unique_ptr<io::output_stream> out_;
};

} // namespace sink
} // namespace vast

#endif
