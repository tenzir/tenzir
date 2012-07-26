#ifndef VAST_FS_EXCEPTION_H
#define VAST_FS_EXCEPTION_H

#include <vast/exception.h>

namespace vast {
namespace fs {

/// The base class for all exceptions in the filesystem layer.
struct exception : public vast::exception
{
  exception() = default;
  exception(char const* msg);
  exception(std::string const& msg);
};

/// Thrown when an error with a directory occurs.
struct dir_exception : public exception
{
  dir_exception(char const* msg, std::string const& dir);
};

/// Thrown when an error with a file occurs.
struct file_exception : public exception
{
  file_exception(char const* msg, std::string const& file);
};

} // namespace fs
} // namespace vast

#endif
