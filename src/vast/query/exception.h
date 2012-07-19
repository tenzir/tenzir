#ifndef VAST_QUERY_EXCEPTION_H
#define VAST_QUERY_EXCEPTION_H

#include <vast/exception.h>

namespace vast {
namespace query {

/// The base class for all exceptions in the query layer.
struct exception : public vast::exception
{
    exception() = default;
    exception(std::string const& msg);
};

/// Thrown when a query does not parse correctly.
struct syntax_exception : public exception
{
    syntax_exception(std::string const& query);
};

/// Thrown when a query has a semantic error, e.g., a type mismatch between the
/// operands.
struct semantic_exception : public exception
{
    semantic_exception(char const* error, std::string const& query);
};

} // namespace query
} // namespace vast

#endif
