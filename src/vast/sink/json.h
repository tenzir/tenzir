#ifndef VAST_SINK_JSON_H
#define VAST_SINK_JSON_H

#include "vast/sink/base.h"
#include "vast/sink/stream.h"

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
  std::string name() const;

private:
  path dir_;
  stream stream_;
};

} // namespace sink
} // namespace vast

#endif
