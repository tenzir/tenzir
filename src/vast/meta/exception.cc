#include <vast/meta/exception.h>

#include <sstream>

namespace vast {
namespace meta {

exception::exception(char const* msg)
  : vast::exception(msg)
{
};


taxonomy_exception::taxonomy_exception(char const* msg)
  : exception(msg)
{
};


semantic_exception::semantic_exception(char const* msg)
  : taxonomy_exception(msg)
{
};

} // namespace meta
} // namespace vast
