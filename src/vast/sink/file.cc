#include "vast/sink/file.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

file::file(std::string const& filename)
  : file_(filename)
{
  VAST_LOG_VERBOSE("spawning file sink @" << id() << " for file " << filename);

  if (! file_)
    VAST_LOG_VERBOSE("file sink @" << id() << " cannot write to " << filename);
}

} // namespace sink
} // namespace vast
