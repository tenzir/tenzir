#include "vast/store/exception.h"

#include <sstream>

namespace vast {
namespace store {

exception::exception()
{
};

exception::exception(char const* msg)
  : vast::exception(msg)
{
};

exception::exception(std::string const& msg)
  : vast::exception(msg)
{
};

exception::~exception() noexcept
{
};


archive_exception::archive_exception()
{
};

archive_exception::archive_exception(char const* msg)
  : exception(msg)
{
};

archive_exception::~archive_exception() noexcept
{
};


segment_exception::segment_exception(char const* msg)
  : exception(msg)
{
};

segment_exception::~segment_exception() noexcept
{
};

} // namespace store
} // namespace vast
