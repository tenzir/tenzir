#ifndef VAST_META_EXCEPTION_H
#define VAST_META_EXCEPTION_H

#include <vast/exception.h>

namespace vast {
namespace meta {

/// The base class for all exceptions in the meta layer.
struct exception : public vast::exception
{
  exception() = default;
  exception(char const* msg);
};

/// Thrown for taxonomy-related errors
struct taxonomy_exception : public exception
{
  taxonomy_exception() = default;
  taxonomy_exception(char const* msg);
};

/// Syntax error in the taxonomy.
struct syntax_exception : public taxonomy_exception
{
  syntax_exception() = default;
};

/// Semantic error in the taxonomy.
struct semantic_exception : public taxonomy_exception
{
  semantic_exception(char const* msg);
};

} // namespace meta
} // namespace vast

#endif
