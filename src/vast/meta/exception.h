#ifndef VAST_META_EXCEPTION_H
#define VAST_META_EXCEPTION_H

#include "vast/exception.h"

namespace vast {
namespace meta {

/// The base class for all exceptions in the meta layer.
struct exception : public vast::exception
{
    exception();
    exception(char const* msg);
    virtual ~exception() noexcept;
};

/// Thrown for taxonomy-related errors
struct taxonomy_exception : public exception
{
    taxonomy_exception();
    taxonomy_exception(char const* msg);
    virtual ~taxonomy_exception() noexcept;
};

/// Syntax error in the taxonomy.
struct syntax_exception : public taxonomy_exception
{
    syntax_exception();
    virtual ~syntax_exception() noexcept;
};

/// Semantic error in the taxonomy.
struct semantic_exception : public taxonomy_exception
{
    semantic_exception(char const* msg);
    virtual ~semantic_exception() noexcept;
};

} // namespace meta
} // namespace vast

#endif
