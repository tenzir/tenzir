#ifndef VAST_UTIL_PARSER_STREAMER_H
#define VAST_UTIL_PARSER_STREAMER_H

#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/support_multi_pass.hpp>

namespace vast {
namespace util {
namespace parser {

typedef std::istream_iterator<char> istream_iterator;
typedef boost::spirit::multi_pass<
    istream_iterator
  , boost::spirit::iterator_policies::default_policy<
        boost::spirit::iterator_policies::first_owner
      , boost::spirit::iterator_policies::no_check
      , boost::spirit::iterator_policies::buffering_input_iterator
      , boost::spirit::iterator_policies::split_std_deque
    >
> multi_pass_iterator;

/// A stream parser that performs a one-time pass over the input and invokes a
/// user-provided callback for each parsed object.
template <
    template <typename> class Grammar
  , template <typename> class Skipper
  , typename Attribute>
class streamer
{
public:
  streamer(std::istream& in)
    : first_(istream_iterator(in))
    , last_(istream_iterator())
  {
  }

  bool extract(Attribute& attr)
  {
    return boost::spirit::qi::phrase_parse(
        first_, last_, grammar_, skipper_, attr);
  }

  bool done() const
  {
    return first_ == last_;
  }

private:
  Grammar<multi_pass_iterator> grammar_;
  Skipper<multi_pass_iterator> skipper_;
  multi_pass_iterator first_;
  multi_pass_iterator last_;
};

} // namespace parser
} // namespace util
} // namespace vast

#endif
