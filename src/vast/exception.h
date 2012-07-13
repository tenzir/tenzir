#ifndef VAST_EXCEPTION_H
#define VAST_EXCEPTION_H

#include "vast/exception.h"

#include <exception>
#include <string>

namespace vast {

/// The base class for all exception thrown by VAST.
class exception : public std::exception
{
public:
  exception();
  exception(char const* msg);
  exception(std::string const& msg);
  virtual ~exception() noexcept;

  virtual char const* what() const noexcept;

protected:
  std::string msg_;
};

struct config_exception : public exception
{
  config_exception(char const* msg, char const* option);
  config_exception(char const* msg, char const* opt1, char const* opt2);
  virtual ~config_exception() noexcept;
};

} // namespace vast

#endif
