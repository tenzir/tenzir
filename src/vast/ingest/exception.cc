#include <vast/ingest/exception.h>

namespace vast {
namespace ingest {

exception::exception(char const* msg)
  : vast::exception(msg)
{
};

exception::exception(std::string const& msg)
  : vast::exception(msg)
{
};


parse_exception::parse_exception(char const* msg)
  : exception(msg)
{
};

} // namespace store
} // namespace vast
