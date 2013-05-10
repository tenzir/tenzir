#ifndef VAST_SINK_FILE_H
#define VAST_SINK_FILE_H

#include <fstream>
#include <cppa/cppa.hpp>
#include <ze/fwd.h>
#include "vast/sink/synchronous.h"

namespace vast {
namespace sink {

/// A file that transforms file contents into events.
class file : public synchronous
{
public:
  /// Constructs a file sink.
  /// @param filename The name of the file to write to.
  file(std::string const& filename);

protected:
  virtual bool write(ze::event const& event) = 0;

  std::ofstream file_;
};

} // namespace sink
} // namespace vast

#endif
