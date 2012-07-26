#include <vast/fs/exception.h>

#include <sstream>

namespace vast {
namespace fs {

exception::exception(char const* msg)
  : vast::exception(msg)
{
};

exception::exception(std::string const& msg)
  : vast::exception(msg)
{
};


dir_exception::dir_exception(char const* msg, std::string const& dir)
  : exception(msg)
{
  std::ostringstream oss;
  oss << msg << " (" << dir << ')';
  msg_ = oss.str();
};

file_exception::file_exception(char const* msg, std::string const& file)
  : exception(msg)
{
  std::ostringstream oss;
  oss << msg << " (" << file << ')';
  msg_ = oss.str();
};

} // namespace fs
} // namespace vast
