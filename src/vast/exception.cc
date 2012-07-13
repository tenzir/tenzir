#include "vast/exception.h"

#include <sstream>

namespace vast {

exception::exception()
{
};

exception::exception(char const* msg)
  : msg_(msg)
{
};

exception::exception(std::string const& msg)
  : msg_(msg)
{
};

exception::~exception() noexcept
{
};

char const* exception::what() const noexcept
{
  return msg_.data();
};


config_exception::config_exception(char const* msg, char const* option)
{
  std::ostringstream oss;
  oss << msg << " (--" << option << ')';
  msg_ = oss.str();
};

config_exception::config_exception(char const* msg, char const* opt1,
                                   char const* opt2)
{
  std::ostringstream oss;
  oss << msg << " (--" << opt1 << " and --" << opt2 << ')';
  msg_ = oss.str();
};

config_exception::~config_exception() noexcept
{
};

} // namespace vast
