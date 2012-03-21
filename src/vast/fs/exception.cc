#include "vast/fs/exception.h"

#include <sstream>

namespace vast {
namespace fs {

exception::exception()
{
};

exception::exception(char const* msg)
  : vast::exception(msg)
{
};

exception::~exception() noexcept
{
};


dir_exception::dir_exception(char const* msg, char const* dir)
  : exception(msg)
{
    std::ostringstream oss;
    oss << msg << " (directory" << dir << ')';
    msg_ = oss.str();
};

dir_exception::~dir_exception() noexcept
{
};

} // namespace fs
} // namespace vast
