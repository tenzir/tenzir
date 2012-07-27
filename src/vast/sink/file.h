#ifndef VAST_SINK_FILE_H
#define VAST_SINK_FILE_H

#include <cassert>
#include <fstream>
#include <cppa/cppa.hpp>
#include <ze/forward.h>
#include "vast/event_sink.h"

namespace vast {
namespace sink {

/// A file that transforms file contents into events.
class file : public event_sink
{
public:
  /// Spawns a file sink.
  /// @param filename The name of the file to write to.
  file(std::string const& filename);

protected:
  virtual bool write(ze::event const& event) = 0;

  std::ofstream file_;
};


} // namespace sink
} // namespace vast

#endif
