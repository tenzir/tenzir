#ifndef VAST_DETAIL_PARSER_ERROR_HANDLER_H
#define VAST_DETAIL_PARSER_ERROR_HANDLER_H

#include <string>
#include <vector>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_function.hpp>
#include "vast/logger.h"

namespace vast {
namespace detail {
namespace parser {

/// A parser error handler that uses the logger to report the parse error.
template <typename Iterator>
struct error_handler
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  error_handler(Iterator first, Iterator last)
    : first(first), last(last)
  {
  }

  template <typename Production, typename ...Args>
  void set(Production& production, Args&& ...args) const
  {
    using boost::spirit::qi::on_error;
    using boost::spirit::qi::fail;
    typedef boost::phoenix::function<error_handler<Iterator>> functor;
    on_error<fail>(production, functor(*this)(std::forward<Args>(args)...));
  }

  template <typename Production>
  void operator()(Production const& production, Iterator err_pos) const
  {
    int line;
    Iterator line_start = get_pos(err_pos, line);
    if (err_pos != last)
    {
      VAST_LOG_ERROR("parse error, expecting " << production <<
                     " at line " << line << ':');
      VAST_LOG_ERROR(get_line(line_start));
      VAST_LOG_ERROR(std::string(err_pos - line_start, ' ') << '^');
    }
    else
    {
      VAST_LOG_ERROR("unexpected end of input in " << production <<
                     " at line " << line);
    }
  }

  Iterator get_pos(Iterator err_pos, int& line) const
  {
    line = 1;
    Iterator i = first;
    Iterator line_start = first;
    while (i != err_pos)
    {
      bool eol = false;
      if (i != err_pos && *i == '\r')
      {
        eol = true;
        line_start = ++i;
      }

      if (i != err_pos && *i == '\n')
      {
        eol = true;
        line_start = ++i;
      }

      if (eol)
        ++line;
      else
        ++i;
    }

    return line_start;
  }

  std::string get_line(Iterator err_pos) const
  {
    Iterator i = err_pos;
    while (i != last && (*i != '\r' && *i != '\n'))
      ++i;

    return std::string(err_pos, i);
  }

  Iterator first;
  Iterator last;
  std::vector<Iterator> iters;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
