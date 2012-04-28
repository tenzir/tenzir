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
    typedef Attribute attribute;
    typedef std::function<void(Attribute const&)> callback_type;

    streamer(callback_type callback)
      : callback_(callback)
    {
    }

    bool extract(std::istream& in)
    {
        namespace qi = boost::spirit::qi;
        auto f = multi_pass_iterator(istream_iterator(in));
        auto l = multi_pass_iterator(istream_iterator());
        auto action = 
            [&](Attribute const& x, qi::unused_type, qi::unused_type)
            {
                callback_(x);
            };

        Grammar<multi_pass_iterator> grammar;
        Skipper<multi_pass_iterator> skipper;
        bool success = true;
        while (success && f != l)
            success = qi::phrase_parse(f, l, +grammar[action], skipper);

        return success;
    }

private:
    callback_type callback_;
};

} // namespace parser
} // namespace util
} // namespace vast

#endif
