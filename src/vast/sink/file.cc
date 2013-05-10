#include "vast/sink/file.h"

#include <ze/event.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

file::file(std::string const& filename)
  : file_(filename)
{
  LOG(verbose, emit)
    << "spawning file sink @" << id() << " for file " << filename;

  if (! file_)
    LOG(error, emit)
      << "file sink @" << id() << " cannot write to " << filename;
}

} // namespace sink
} // namespace vast
