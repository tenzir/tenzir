#ifndef VAST_QUERY_PARSER_ERROR_HANDLER_H
#define VAST_QUERY_PARSER_ERROR_HANDLER_H

#include <iostream>
#include <string>
#include <vector>

namespace vast {
namespace query {
namespace parser {

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

    template <typename What>
    void operator()(What const& what, Iterator err_pos) const
    {
        int line;
        Iterator line_start = get_pos(err_pos, line);
        if (err_pos != last)
        {
            std::cout
                << "parse error, expecting " << what
                << " line " << line << ':' << std::endl
                << get_line(line_start) << std::endl;

            while (line_start++ != err_pos)
                std::cout << ' ';
            std::cout << '^' << std::endl;
        }
        else
        {
            std::cout << "unexpected end of query ";
            std::cout << what << " line " << line << std::endl;
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

} // namespace ast
} // namespace query
} // namespace vast

#endif
