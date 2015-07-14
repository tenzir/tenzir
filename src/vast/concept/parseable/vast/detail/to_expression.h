#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TO_EXPRESSION_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_TO_EXPRESSION_H

#include <string>

namespace vast {

class expression;

namespace detail {

trial<expression> to_expression(std::string const& str);

} // namespace detail
} // namespace vast

#endif
