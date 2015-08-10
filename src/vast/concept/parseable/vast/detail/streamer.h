#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_STREAMER_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_STREAMER_H

#include "vast/concept/parseable/vast/detail/boost.h"

namespace vast {
namespace detail {
namespace parser {

using istream_iterator = std::istream_iterator<char>;
using multi_pass_iterator =
  boost::spirit::multi_pass<
    istream_iterator,
    boost::spirit::iterator_policies::default_policy<
      boost::spirit::iterator_policies::first_owner,
      boost::spirit::iterator_policies::no_check,
      boost::spirit::iterator_policies::buffering_input_iterator,
      boost::spirit::iterator_policies::split_std_deque
    >
  >;

/// A stream parser that performs a single pass over the input and extracts
/// attributes until done.
template <
  template <typename> class Grammar,
  template <typename> class Skipper,
  typename Attribute
>
class streamer {
public:
  streamer(std::istream& in)
    : first_(istream_iterator(in)), last_(istream_iterator()) {
  }

  bool extract(Attribute& attr) {
    return boost::spirit::qi::phrase_parse(first_, last_, grammar_, skipper_,
                                           attr);
  }

  bool done() const {
    return first_ == last_;
  }

private:
  Grammar<multi_pass_iterator> grammar_;
  Skipper<multi_pass_iterator> skipper_;
  multi_pass_iterator first_;
  multi_pass_iterator last_;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
