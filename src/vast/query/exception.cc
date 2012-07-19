#include <vast/query/exception.h>

#include <sstream>

namespace vast {
namespace query {

exception::exception(std::string const& msg)
  : vast::exception(msg)
{
};


syntax_exception::syntax_exception(std::string const& query)
{
  std::ostringstream oss;
  oss << "invalid query syntax (" << query << ')';
  msg_ = oss.str();
};


semantic_exception::semantic_exception(char const* error, std::string const& query)
{
  std::ostringstream oss;
  oss << error << " (" << query << ')';
  msg_ = oss.str();
};


} // namespace query
} // namespace vast
