#ifndef VAST_INGEST_EXCEPTION_H
#define VAST_INGEST_EXCEPTION_H

#include <vast/exception.h>

namespace vast {
namespace ingest {

/// The base class for all exceptions in the storage layer.
struct exception : public vast::exception
{
  exception() = default;
  exception(char const* msg);
  exception(std::string const& msg);
};

/// Thrown when a parse error occurs while ingesting input data.
struct parse_exception : public exception
{
  parse_exception() = default;
  parse_exception(char const* msg);
};

} // namespace ingest
} // namespace vast

#endif
