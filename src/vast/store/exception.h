#ifndef VAST_STORE_EXCEPTION_H
#define VAST_STORE_EXCEPTION_H

#include <vast/exception.h>

namespace vast {
namespace store {

/// The base class for all exceptions in the storage layer.
struct exception : public vast::exception
{
  exception() = default;
  exception(char const* msg);
  exception(std::string const& msg);
};

/// Thrown when an error with an archive occurs.
struct archive_exception : public exception
{
  archive_exception() = default;
  archive_exception(char const* msg);
};

/// Thrown when an error with a segment occurs.
struct segment_exception : public exception
{
  segment_exception(char const* msg);
};

} // namespace store
} // namespace vast

#endif
