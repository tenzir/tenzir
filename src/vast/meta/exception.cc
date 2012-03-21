#include "vast/meta/exception.h"

#include <sstream>

namespace vast {
namespace meta {

exception::exception()
{
};

exception::exception(char const* msg)
  : vast::exception(msg)
{
};

exception::~exception() noexcept
{
};


taxonomy_exception::taxonomy_exception()
{
};

taxonomy_exception::taxonomy_exception(char const* msg)
  : exception(msg)
{
};

taxonomy_exception::~taxonomy_exception() noexcept
{
};


syntax_exception::syntax_exception()
{
};

syntax_exception::~syntax_exception() noexcept
{
};


semantic_exception::semantic_exception(char const* msg)
  : taxonomy_exception(msg)
{
};

semantic_exception::~semantic_exception() noexcept
{
};

} // namespace meta
} // namespace vast
