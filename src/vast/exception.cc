#include "vast/exception.h"

#include <sstream>
#include "vast/operator.h"
#include "vast/to_string.h"

namespace vast {

exception::exception(char const* msg)
  : msg_(msg)
{
}

exception::exception(std::string const& msg)
  : msg_(msg)
{
}

char const* exception::what() const noexcept
{
  return msg_.data();
}

namespace error {

fs::fs(char const* msg)
  : exception(msg)
{
}

fs::fs(char const* msg, std::string const& filename)
{
  std::ostringstream oss;
  oss << "file " << filename << ": " << msg;
  msg_ = oss.str();
}

network::network(char const* msg)
  : exception(msg)
{
}

#ifdef VAST_HAVE_BROCCOLI
broccoli::broccoli(char const* msg)
  : network(msg)
{
}
#endif

config::config(char const* msg, char const* option)
{
  std::ostringstream oss;
  oss << msg << " (--" << option << ')';
  msg_ = oss.str();
}

config::config(char const* msg, char const* opt1, char const* opt2)
{
  std::ostringstream oss;
  oss << msg << " (--" << opt1 << " and --" << opt2 << ')';
  msg_ = oss.str();
}

ingest::ingest(char const* msg)
  : exception(msg)
{
}

ingest::ingest(std::string const& msg)
  : exception(msg)
{
}

parse::parse(char const* msg)
  : ingest(msg)
{
}

parse::parse(char const* msg, size_t line)
  : ingest(msg)
{
  std::ostringstream oss;
  oss << "line " << line << ": " << msg;
  msg_ = oss.str();
}

segment::segment(char const* msg)
  : exception(msg)
{
}

query::query(char const* msg)
  : exception(msg)
{
}

query::query(char const* msg, std::string const& expr)
{
  std::ostringstream oss;
  oss << msg << "'" << expr << "'";
  msg_ = oss.str();
}

schema::schema(char const* msg)
  : exception(msg)
{
}

index::index(char const* msg)
  : exception(msg)
{
}

operation::operation(char const* msg, arithmetic_operator op)
{
  std::ostringstream oss;
  oss << msg << ": " << to_string(op);
  msg_ = oss.str();
}

operation::operation(char const* msg, boolean_operator op)
{
  std::ostringstream oss;
  oss << msg << ": " << to_string(op);
  msg_ = oss.str();
}

operation::operation(char const* msg, relational_operator op)
{
  std::ostringstream oss;
  oss << msg << ": " << to_string(op);
  msg_ = oss.str();
}

} // namespace error
} // namespace vast
