#ifndef VAST_ACTOR_SINK_ASCII_H
#define VAST_ACTOR_SINK_ASCII_H

#include <iosfwd>
#include <memory>

#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

/// A sink dumping events in plain ASCII.
class ascii : public base<ascii>
{
public:
  /// Spawns an ASCII sink.
  /// @param out The output stream.
  ascii(std::unique_ptr<std::ostream> out);

  bool process(event const& e);

  void flush();

private:
  std::unique_ptr<std::ostream> out_;
};

} // namespace sink
} // namespace vast

#endif
