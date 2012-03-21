#ifndef VAST_COMM_EXCEPTION_H
#define VAST_COMM_EXCEPTION_H

#include "vast/exception.h"

namespace vast {
namespace comm {

/// The base class for all exceptions in the communication layer.
struct exception : public vast::exception
{
    exception();
    exception(char const* msg);
    virtual ~exception() noexcept;
};

/// Thrown when an error with Broccoli occurs.
struct broccoli_exception : public exception
{
    broccoli_exception();
    broccoli_exception(char const* msg);
    virtual ~broccoli_exception() noexcept;
};

/// Thrown for errors with Broccoli types.
struct broccoli_type_exception : public broccoli_exception
{
    broccoli_type_exception(char const* msg, int type);
    virtual ~broccoli_type_exception() noexcept;
};

} // namespace comm
} // namespace vast

#endif
