#ifndef VAST_ACTOR_SINK_JSON_H
#define VAST_ACTOR_SINK_JSON_H

#include <iosfwd>
#include <memory>

#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

/// A sink generating JSON output.
class json : public base<json> {
public:
  /// Spawns a JSON sink.
  /// @param out The output stream.
  json(std::unique_ptr<std::ostream> out);

  bool process(event const& e);

  void flush();

private:
  std::unique_ptr<std::ostream> out_;
};

} // namespace sink
} // namespace vast

#endif
