#include "vast/comm/exception.h"

#include <sstream>

namespace vast {
namespace comm {

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


broccoli_exception::broccoli_exception()
{
};

broccoli_exception::broccoli_exception(char const* msg)
  : exception(msg)
{
};

broccoli_exception::~broccoli_exception() noexcept
{
};


broccoli_type_exception::broccoli_type_exception(char const* msg, int type)
{
    std::ostringstream oss;
    oss << msg << " (" << type << ')';
    msg_ = oss.str();
};

broccoli_type_exception::~broccoli_type_exception() noexcept
{
};

} // namespace comm
} // namespace vast
