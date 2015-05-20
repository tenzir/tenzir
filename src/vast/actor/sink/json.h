#ifndef VAST_ACTOR_SINK_JSON_H
#define VAST_ACTOR_SINK_JSON_H

#include "vast/actor/sink/base.h"

namespace vast {

namespace io { class output_stream; }

namespace sink {

/// A sink generating JSON output.
class json : public base<json>
{
public:
  /// Spawns a JSON sink.
  /// @param out The output stream.
  json(std::unique_ptr<io::output_stream> out);

  bool process(event const& e);

  void flush();

private:
  std::unique_ptr<io::output_stream> out_;
};

} // namespace sink
} // namespace vast

#endif
