#ifndef VAST_STORE_EXCEPTION_H
#define VAST_STORE_EXCEPTION_H

#include "vast/exception.h"

namespace vast {
namespace store {

/// The base class for all exceptions in the storage layer.
struct exception : public vast::exception
{
    exception();
    exception(char const* msg);
    exception(std::string const& msg);
    virtual ~exception() noexcept;
};

/// Thrown when an error with an archive occurs.
struct archive_exception : public exception
{
    archive_exception();
    archive_exception(char const* msg);
    virtual ~archive_exception() noexcept;
};

/// Thrown when an error with a segment occurs.
struct segment_exception : public exception
{
    segment_exception(char const* msg);
    virtual ~segment_exception() noexcept;
};

} // namespace store
} // namespace vast

#endif
