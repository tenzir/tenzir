#ifndef VAST_DETAIL_PARSER_ERROR_HANDLER_H
#define VAST_DETAIL_PARSER_ERROR_HANDLER_H

#include <string>
#include <vector>
#include "vast/detail/parser/boost.h"

namespace vast {
namespace detail {
namespace parser {

/// A parser error handler that exposes the error as string.
template <typename Iterator>
struct error_handler
{
  template <typename, typename>
  struct result
  {
    typedef void type;
  };

  error_handler(Iterator first, Iterator last, std::string& error)
    : first{first}, last{last}, error{error}
  {
  }

  template <typename Production, typename ...Args>
  void set(Production& production, Args&& ...args) const
  {
    using boost::spirit::qi::on_error;
    using boost::spirit::qi::fail;
    using functor = boost::phoenix::function<error_handler<Iterator>>;
    on_error<fail>(production, functor(*this)(std::forward<Args>(args)...));
  }

  template <typename Production>
  void operator()(Production const& production, Iterator err_pos) const
  {
    int line;
    Iterator line_start = get_pos(err_pos, line);
    std::stringstream ss;

    if (err_pos != last)
      ss
        << "parse error, expecting " << production
        << " at line " << line << ":\n"
        << get_line(line_start) << '\n'
        << std::string(err_pos - line_start, ' ') << '^';
    else
      ss << "unexpected end of input in " << production << " at line " << line;

    error = ss.str();
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
  std::string& error;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
